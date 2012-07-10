package nl.vu.datalayer.hbase.bulkload;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import net.fortytwo.sesametools.nquads.NQuadsParser;
import nl.vu.datalayer.hbase.exceptions.NumericalRangeException;
import nl.vu.datalayer.hbase.id.BaseId;
import nl.vu.datalayer.hbase.id.DataPair;
import nl.vu.datalayer.hbase.id.HBaseValue;
import nl.vu.datalayer.hbase.id.TypedId;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.StatementCollector;

public class TripleToResource {
	
	public static final String RESOURCE_IDS_DIR = "resourceIds";
	public static final String ID2STRING_DIR = "idStringAssoc";
	public static final String DEFAULT_CONTEXT = "http://DEFAULT_CONTEXT";
	
	public static final String EXCEPTION_GROUP = "ExceptionGroup";
	
	public static class TripleToResourceMapper extends 
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Writable, HBaseValue, DataPair>
	{
		private static RDFParser parser = null;
		private static StatementCollector collector = null;
		private static ArrayList<Statement> statements = null;
		
		private int partitionId;
		private long localRowCount = 0;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			statements = new ArrayList<Statement>();
			collector = new StatementCollector(statements);
			parser = new NQuadsParser();
			//parser = new TurtleParser();
			parser.setRDFHandler(collector);
			parser.setPreserveBNodeIDs(true);
			
			partitionId = context.getConfiguration().getInt("mapred.task.partition", 0);
			System.out.println("Finished configuration "+partitionId);
			System.out.println( context.getTaskAttemptID() +  " - "+ ((FileSplit)context.getInputSplit()).getPath());
		}
		
		@Override
		public void map(LongWritable key, Writable value, Context context) throws IOException, InterruptedException{
			try {
				Statement s = getNewStatement(value);
				
				BaseId quadId = new BaseId(partitionId, localRowCount);
				localRowCount++;
				
				DataPair []outputPairs = createDataPairs(quadId);
				
				emitOutputPairs(context, s, outputPairs);
				
			} catch (RDFParseException e) {
				System.err.println("Unexpected input at line: "+key.get()+" : "+e.getMessage());
				context.getCounter(EXCEPTION_GROUP, "RDFParseExceptions").increment(1);
			} catch (RDFHandlerException e) {
				System.err.println("Line: "+key.get()+"; "+e.getMessage());
				context.getCounter(EXCEPTION_GROUP, "RDFHandlerExceptions").increment(1);
			} 
		}

		final private void emitOutputPairs(Context context, Statement s, DataPair[] dataPairs) throws IOException, InterruptedException {
			context.write(new HBaseValue(s.getSubject()), dataPairs[DataPair.S]);
			context.write(new HBaseValue(s.getPredicate()), dataPairs[DataPair.P]);
			context.write(new HBaseValue(s.getObject()), dataPairs[DataPair.O]);
			
			Value quadContext;
			if (s.getContext() == null){
				quadContext = new URIImpl(DEFAULT_CONTEXT);
			}
			else{
				quadContext = s.getContext();
			}
			context.write(new HBaseValue(quadContext), dataPairs[DataPair.C]);
		}
		
		final private DataPair []createDataPairs(BaseId id){
			DataPair []outputPair = new DataPair[4];
			for (int i = 0; i < outputPair.length; i++) {
				outputPair[i] = new DataPair(id, (byte)i);
			}
			return outputPair;
		}

		final private Statement getNewStatement(Writable value) throws IOException, RDFParseException, RDFHandlerException {
			InputStream in = new ByteArrayInputStream(value.toString().getBytes());
			parser.parse(in, "");
			if (statements.size() == 0){
				return null;
			}							
			Statement ret = statements.get(statements.size()-1);
			if (statements.size() == 5000){
				statements.clear();
			}
			
			return ret;
		}
	
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("Mapper Partition: "+partitionId+"; "+localRowCount);
		}		
	}
	
	
	public static class TripleToResourceReducer extends org.apache.hadoop.mapreduce.Reducer<HBaseValue, DataPair, TypedId, DataPair>
	{
			private int partitionId;
			private long localRowCount = 0;
			private static ValueFactory valFact;
			
			private static SequenceFile.Writer id2StringWriter;	
			
			/**
			 * Counter group with 3 counters: URIs, bNodes and Literals 
			 */
			public static final String ELEMENT_TYPE_GROUP = "ElemsGroup";
			
			/**
			 * Counter group that build a byte histogram* of the last characters of input keys
			 * 
			 * note: the histogram is not complete because hadoop limits the number of counters to 120: 
			 * so 2 byte values are counted with the same counter, and we assume that they are equally distributed
			 * TODO change in future releases when counter limit can be configured
			 */
			//public static final String HISTOGRAM_GROUP = "HistogramGroup";
			public static final String NUMERICAL_GROUP = "NumericalGroup";		
			
			private String schemaSuffix;
			
			@Override
			protected void setup(Context context) throws IOException, InterruptedException {
				FileSystem fs;
	
				Configuration config = context.getConfiguration();
				partitionId = config.getInt("mapred.task.partition", 0);
				schemaSuffix = config.get("schemaSuffix");
				String outputPath = config.get("outputPath");
				fs = FileSystem.get(config);
	
				valFact = new ValueFactoryImpl();
	
				Path sideEffectFile = new Path(FileOutputFormat.getWorkOutputPath(context), 
						ID2STRING_DIR + String.format("/%05d", partitionId));
				id2StringWriter = SequenceFile.createWriter(fs, config, sideEffectFile,
				//new Path(outputPath+ID2STRING_DIR+"/"+context.getTaskAttemptID()),
				//new Path("idStringAssoc"+String.format("/%05d", partitionId)),
						ImmutableBytesWritable.class, HBaseValue.class, CompressionType.RECORD);
			}
			
			@Override
			protected void cleanup(Context context) throws IOException, InterruptedException {
				//id2StringWriter.syncFs();
				id2StringWriter.close();
				
				System.out.println("Closed file writer");
				HBPrefixMatchSchema.updateCounter(partitionId, localRowCount, schemaSuffix);
				System.out.println("Counter updated for partition "+partitionId);
			}

			//for testing purposes
			public static void setValueVatory(ValueFactory valFactory){
				valFact = valFactory;
			}
			
			/*public void updateHistogram(Context context, Value literal){
				byte []bytes = literal.stringValue().getBytes();
				context.getCounter(HISTOGRAM_GROUP, 
						String.format("%02x", (short)bytes[bytes.length-1] & 0xfe)).increment(1);
				
			}*/

			@Override
			public void reduce(HBaseValue key, Iterable<DataPair> values, Context context) throws IOException, InterruptedException {
				//generate a new id for the triple element represented by the key
				try{
					TypedId newId = generateTypedId(context, key.getUnderlyingValue());				
					saveTypeIdToFile(newId, key, context);
					
					//emit (elementID, <tripleID, position>) elements
					for (DataPair dataPair : values) {
						context.write(newId, dataPair);
					}	
				}
				catch(NumericalRangeException e){
					System.err.println("Numerical not in expected range: "+e.getMessage());
					context.getCounter(EXCEPTION_GROUP, "RangeExceptions").increment(1);
					return;
				}
			}

			final private TypedId generateTypedId(Context context, Value elem) throws IOException, NumericalRangeException {
				if (elem instanceof Literal){//Literals				
					return handleLiteral(context, elem);
				}
				else{//non-Literals
					//context.getCounter(HISTOGRAM_GROUP, String.format("%02x", (short)elemBytes[elemBytes.length-1] & 0xfe)).increment(1);
					return handleNonLiteral(context, elem);
				}
			}
			
			final private TypedId handleLiteral(Context context, Value elem) throws IOException, NumericalRangeException{
				Counter c = context.getCounter(ELEMENT_TYPE_GROUP, "Literals"); 
				c.increment(1);
				Literal l = (Literal)elem;
				if (l.getDatatype() == null){//the Literals with no datatype are considered Strings
					return new TypedId(partitionId, localRowCount);
					//updateHistogram(context, elem);
				}
				else{//we have a datatype
					TypedId newId = TypedId.createNumerical(l);
					if (newId == null){//non-numerical literal
						newId = new TypedId(partitionId, localRowCount);
						//updateHistogram(context, elem);
					}
					return newId;
				}
			}
			
			final private TypedId handleNonLiteral(Context context, Value elem){
				increaseNonLiteralCounters(elem, context);
				return new TypedId(partitionId, localRowCount);
			}
			
			final private void increaseNonLiteralCounters(Value elem, Context context){
				if (elem instanceof BNode){
					context.getCounter(ELEMENT_TYPE_GROUP, "Blanks").increment(1);
				}
				else{
					context.getCounter(ELEMENT_TYPE_GROUP, "URIs").increment(1);
				}
			}	
			
			final private void saveTypeIdToFile(TypedId newId, HBaseValue value, Context context) throws IOException{
				if (newId.getType() != TypedId.NUMERICAL){
					id2StringWriter.append(new ImmutableBytesWritable(newId.getContent()), value);
					/*if (localRowCount % 1000 == 0){
						id2StringWriter.syncFs();
					}*/
					localRowCount++;
					context.getCounter(NUMERICAL_GROUP, "NonNumericals").increment(1);
				}
				else{
					context.getCounter(NUMERICAL_GROUP, "Numericals").increment(1);
				}
			}
	}

	public static void main(String[] args) {		
		Job j1;
		try {
			//Path p = new Path(new Path("/test1"), new Path(ID2STRING_DIR));
			//System.out.println(p);
		
			if (args.length != 3){
				System.out.println("Usage: TripleToResource <inputPath> <inputSizeEstimate in MB> <outputPath>");
				return;
			}
			
			Path input = new Path(args[0]);
			String outputPath = args[2];
			
			Path resourceIds = new Path(outputPath+TripleToResource.RESOURCE_IDS_DIR);
			
			j1 = BulkLoad.createTripleToResourceJob(input, resourceIds, Integer.parseInt(args[1]));
			
			j1.getConfiguration().set("outputPath", outputPath);
			j1.getConfiguration().set("schemaSuffix", "_TEST");
			j1.waitForCompletion(true);
			
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			Path source = new Path(resourceIds, "idStringAssoc");
			Path id2String = new Path(outputPath+"/idStringAssoc");
			FileUtil.copy(fs, source, fs, id2String, true, false, conf);
			
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

	}
}
