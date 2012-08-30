package nl.vu.datalayer.hbase.bulkload;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;

import net.fortytwo.sesametools.nquads.NQuadsParser;
import nl.vu.datalayer.hbase.exceptions.NonNumericalException;
import nl.vu.datalayer.hbase.exceptions.NumericalRangeException;
import nl.vu.datalayer.hbase.id.BaseId;
import nl.vu.datalayer.hbase.id.DataPair;
import nl.vu.datalayer.hbase.id.HBaseValue;
import nl.vu.datalayer.hbase.id.TypedId;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.StatementCollector;

public class QuadBreakDown {
	
	public static final String RESOURCE_IDS_DIR = "resourceIds";
	public static final String ID2STRING_DIR = "idStringAssoc";
	public static final String DEFAULT_CONTEXT = "http://DEFAULT_CONTEXT";
	
	public static final String EXCEPTION_GROUP = "ExceptionGroup";
	public static final int QUAD_MEDIAN_SIZE = 190;
	public static final int QUAD_AVERAGE_SIZE = 234;
	
	public static long getMapOutputRecordSizeEstimate() {
		long baseDataPairSize = BaseId.SIZE+Bytes.SIZEOF_BYTE+//serialized id
								Bytes.SIZEOF_BYTE;//position
		return QUAD_MEDIAN_SIZE+4*baseDataPairSize;
	}
	
	public static class TripleToResourceMapper extends 
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Writable, HBaseValue, DataPair>
	{
		private static RDFParser parser = null;
		private static StatementCollector collector = null;
		private static ArrayList<Statement> statements = null;
		
		private int partitionId;
		private long localRowCount = 0;
		private BaseId quadId = new BaseId();
		private DataPair [] outputPairs = null;
		private HBaseValue hbaseValue = null;
		private URI defaultContext = new URIImpl(DEFAULT_CONTEXT);
		
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
			
			outputPairs = new DataPair[4];
			for (int i = 0; i < outputPairs.length; i++) {
				outputPairs[i] = new DataPair();
			}
			hbaseValue = new HBaseValue();
		}
		
		@Override
		public void map(LongWritable key, Writable value, Context context) throws IOException, InterruptedException{
			try {
				Statement s = getNewStatement(value);
				if (s == null)
					return;
				
				quadId.set(partitionId, localRowCount);
				localRowCount++;
				
				createDataPairs(quadId);
				
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
			hbaseValue.setValue(s.getSubject());
			context.write(hbaseValue, dataPairs[DataPair.S]);
			hbaseValue.setValue(s.getPredicate());
			context.write(hbaseValue, dataPairs[DataPair.P]);
			hbaseValue.setValue(s.getObject());
			context.write(hbaseValue, dataPairs[DataPair.O]);
			
			Value quadContext;
			if (s.getContext() == null){
				quadContext = defaultContext;
			}
			else{
				quadContext = s.getContext();
			}
			hbaseValue.setValue(quadContext);
			context.write(hbaseValue, dataPairs[DataPair.C]);
		}
		
		final private void createDataPairs(BaseId id){
			for (int i = 0; i < outputPairs.length; i++) {
				outputPairs[i].set(id, (byte)i);
			}
		}

		final private Statement getNewStatement(Writable value) throws IOException, RDFParseException, RDFHandlerException {
			parser.parse(new StringReader(value.toString()), "");
			
			if (statements.size() == 0){
				return null;
			}							
			Statement ret = statements.get(statements.size()-1);
			if (statements.size() == 100){
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
			
			private static SequenceFile.Writer id2StringWriter;	
			
			/**
			 * Counter group with 3 counters: URIs, bNodes and Literals 
			 */
			public static final String ELEMENT_TYPE_GROUP = "ElemsGroup";
			
			public static final String NUMERICAL_GROUP = "NumericalGroup";		
			
			private String schemaSuffix;
			private TypedId newId = new TypedId();
			private ImmutableBytesWritable sideEffectFileKey = new ImmutableBytesWritable();
			
			@Override
			protected void setup(Context context) throws IOException, InterruptedException {
				FileSystem fs;
	
				Configuration config = context.getConfiguration();
				partitionId = config.getInt("mapred.task.partition", 0);
				schemaSuffix = config.get("schemaSuffix");
				fs = FileSystem.get(config);
	
				Path sideEffectFile = new Path(FileOutputFormat.getWorkOutputPath(context), 
						ID2STRING_DIR + String.format("/%05d", partitionId));
				id2StringWriter = SequenceFile.createWriter(fs, config, sideEffectFile,
						ImmutableBytesWritable.class, HBaseValue.class, CompressionType.RECORD);
			}
			
			@Override
			protected void cleanup(Context context) throws IOException, InterruptedException {
				id2StringWriter.close();
				
				System.out.println("Closed file writer");
				HBPrefixMatchSchema.updateCounter(partitionId, localRowCount, schemaSuffix);
				System.out.println("Counter updated for partition "+partitionId);
			}

			@Override
			public void reduce(HBaseValue key, Iterable<DataPair> values, Context context) throws IOException, InterruptedException {
				//generate a new id for the triple element represented by the key
				try{
					generateTypedId(context, key.getUnderlyingValue());				
					saveTypeIdToFile(key, context);
					
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

			final private void generateTypedId(Context context, Value elem) throws IOException, NumericalRangeException {
				if (elem instanceof Literal){//Literals				
					handleLiteral(context, elem);
				}
				else{//non-Literals
					handleNonLiteral(context, elem);
				}
			}
			
			final private void handleLiteral(Context context, Value elem) throws IOException, NumericalRangeException{
				Counter c = context.getCounter(ELEMENT_TYPE_GROUP, "Literals"); 
				c.increment(1);
				Literal l = (Literal)elem;
				if (l.getDatatype() == null){//the Literals with no datatype are considered Strings
					newId.set(partitionId, localRowCount);
				}
				else{//we have a datatype
					try {
						newId = TypedId.createNumerical(l);
					} catch (NonNumericalException e) {
						newId.set(partitionId, localRowCount);
					}
				}
			}
			
			final private void handleNonLiteral(Context context, Value elem){
				increaseNonLiteralCounters(elem, context);
				newId.set(partitionId, localRowCount);
			}
			
			final private void increaseNonLiteralCounters(Value elem, Context context){
				if (elem instanceof BNode){
					context.getCounter(ELEMENT_TYPE_GROUP, "Blanks").increment(1);
				}
				else{
					context.getCounter(ELEMENT_TYPE_GROUP, "URIs").increment(1);
				}
			}	
			
			final private void saveTypeIdToFile(HBaseValue value, Context context) throws IOException{
				if (newId.getType() != TypedId.NUMERICAL){
					sideEffectFileKey.set(newId.getContent());
					id2StringWriter.append(sideEffectFileKey, value);
					
					localRowCount++;
					context.getCounter(NUMERICAL_GROUP, "NonNumericals").increment(1);
				}
				else{
					context.getCounter(NUMERICAL_GROUP, "Numericals").increment(1);
				}
			}
	}

	
}
