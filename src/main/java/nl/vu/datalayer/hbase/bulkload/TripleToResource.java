package nl.vu.datalayer.hbase.bulkload;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;

import net.fortytwo.sesametools.nquads.NQuadsParser;
import nl.vu.datalayer.hbase.id.BaseId;
import nl.vu.datalayer.hbase.id.DataPair;
import nl.vu.datalayer.hbase.id.NumericalRangeException;
import nl.vu.datalayer.hbase.id.TypedId;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.ntriples.NTriplesUtil;

public class TripleToResource {
	
	public static final String RESOURCE_IDS_DIR = "/resourceIds";
	public static final String ID2STRING_DIR = "/idStringAssoc";
	public static final String DEFAULT_CONTEXT = "http://DEFAULT_CONTEXT";
	
	public static final String EXCEPTION_GROUP = "ExceptionGroup";
	
	public static class TripleToResourceMapper extends MapReduceBase implements Mapper<LongWritable, Writable, Text, DataPair>
	{
		private static RDFParser parser = null;
		private static StatementCollector collector = null;
		private static ArrayList<Statement> statements = null;
		
		private int partitionId;
		private long localRowCount = 0;
		
		
		
		@Override
		public void configure(JobConf job) {
			statements = new ArrayList<Statement>();
			collector = new StatementCollector(statements);
			parser = new NQuadsParser();
			//parser = new TurtleParser();
			parser.setRDFHandler(collector);
			parser.setPreserveBNodeIDs(true);
			
			partitionId = job.getInt("mapred.task.partition", 0);
			System.out.println("Finished configuration "+partitionId);
		}
		
		public void map(LongWritable key, Writable value,  OutputCollector<Text, DataPair> output, Reporter reporter) throws IOException{
			try {
				InputStream in = new ByteArrayInputStream(value.toString().getBytes());
				parser.parse(in, "");
				
				if (statements.size() == 0){
					return;
				}				
				
				Statement s = statements.get(statements.size()-1);
				//System.out.println(s);
				
				//generate triple ID
				BaseId id = new BaseId(partitionId, localRowCount);
				localRowCount++;
				
				//create output pairs
				DataPair subject = new DataPair(id, DataPair.S);
				DataPair predicate = new DataPair(id, DataPair.P);
				DataPair object = new DataPair(id, DataPair.O);
				DataPair tContext = new DataPair(id, DataPair.C);
				
				//emit output
				output.collect(new Text(s.getSubject().toString()), subject);
				output.collect(new Text(s.getPredicate().toString()), predicate);
				output.collect(new Text(s.getObject().toString()), object);
				
				String quadContext;
				if (s.getContext() == null)
					quadContext = DEFAULT_CONTEXT;
				else
					quadContext = s.getContext().toString();
				output.collect(new Text(quadContext), tContext);
				
				if (statements.size() == 5000){
					statements.clear();
				}
				
			} catch (RDFParseException e) {
				System.err.println("Unexpected input at line: "+key.get()+" : "+e.getMessage());
				reporter.incrCounter(EXCEPTION_GROUP, "RDDParseExceptions", 1);
			} catch (RDFHandlerException e) {
				System.err.println("Line: "+key.get()+"; "+e.getMessage());
				reporter.incrCounter(EXCEPTION_GROUP, "RDDHandlerExceptions", 1);
			} 
		}

		@Override
		public void close() throws IOException {
			System.out.println("Mapper Partition: "+partitionId+"; "+localRowCount);
		}
				
		
	}
	
	/**
	 * @author sever
	 *
	 */
	public static class TripleToResourceReducer extends MapReduceBase implements Reducer<Text, DataPair, TypedId, DataPair>
	{
			private int partitionId;
			private long localRowCount = 0;
			private static ValueFactory valFact;
			
			private static SequenceFile.Writer id2StringWriter;
			
			//public static final String URI_GROUP = "URIGroup";
			public static final String ELEMENT_TYPE_GROUP = "ElemsGroup";
			public static final String HISTOGRAM_GROUP = "HistogramGroup";
			public static final String NUMERICAL_GROUP = "NumericalGroup";		
			
			@Override
			public void configure(JobConf config) {		
				FileSystem fs;
				try {
					partitionId = config.getInt("mapred.task.partition", 0);
					String outputPath = config.get("outputPath");
					fs = FileSystem.get(config);
					
					valFact = new ValueFactoryImpl();
					
					id2StringWriter = SequenceFile.createWriter(fs, config, 
							new Path(outputPath+ID2STRING_DIR+String.format("/%05d", partitionId)),
							//new Path("idStringAssoc"+String.format("/%05d", partitionId)),
							ImmutableBytesWritable.class, Text.class,
							CompressionType.RECORD);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			//for testing purposes
			public static void setValueVatory(ValueFactory valFactory){
				valFact = valFactory;
			}
			
			@Override
			public void close() throws IOException {
				id2StringWriter.syncFs();
				id2StringWriter.close();
				
				System.out.println("Closed file writer");
				HBPrefixMatchSchema.updateCounter(partitionId, localRowCount);
				System.out.println("Counter updated for partition "+partitionId);
			}
			
			public void updateHistogram(Reporter reporter, String literal){
				if (literal.endsWith("\"")){
					reporter.incrCounter(HISTOGRAM_GROUP, String.format("%02x", (short)literal.getBytes()[literal.getBytes().length-2] & 0xfe), 1);
				}
				else{
					reporter.incrCounter(HISTOGRAM_GROUP, String.format("%02x", (short)literal.getBytes()[literal.getBytes().length-1] & 0xfe), 1);
				}
			}

			@Override
			public void reduce(Text key, Iterator<DataPair> it, OutputCollector<TypedId, DataPair> output, Reporter reporter) throws IOException {
				
				//generate a new id for the triple element represented by the key
			
					TypedId newId;
					String elem = key.toString();			
					if (elem.startsWith("\"")){//Literals
						reporter.incrCounter(ELEMENT_TYPE_GROUP, "Literals", 1);
						try{
							Literal l = NTriplesUtil.parseLiteral(elem, valFact);
							if (l.getDatatype() == null){//the Literals with no datatype are considered Strings
								newId = new TypedId(partitionId, localRowCount);
								updateHistogram(reporter, elem);
							}
							else{//we have a datatype
								newId = TypedId.createNumerical(l);
								if (newId == null){//non-numerical literal
									newId = new TypedId(partitionId, localRowCount);
									updateHistogram(reporter, elem);
								}
							}
						}
						catch(NumericalRangeException e){
							System.err.println("Numerical not in expected range: "+e.getMessage());
							reporter.incrCounter(EXCEPTION_GROUP, "RangeExceptions", 1);
							return;
						}
						catch(IllegalArgumentException e){
							System.err.println("Literal not in proper format: "+e.getMessage());
							reporter.incrCounter(EXCEPTION_GROUP, "LiteralExceptions", 1);
							return;
						}	
					}
					else{//non-Literals
						reporter.incrCounter(HISTOGRAM_GROUP, String.format("%02x", (short)elem.getBytes()[elem.getBytes().length-1] & 0xfe), 1);
						if (elem.startsWith("_:")){
							reporter.incrCounter(ELEMENT_TYPE_GROUP, "Blanks", 1);
						}
						else{
							reporter.incrCounter(ELEMENT_TYPE_GROUP, "URIs", 1);
						}
						
						newId = new TypedId(partitionId, localRowCount);
					}
					
					//write the association between IDs and Strings in a SequenceFile
					if (newId.getType() != TypedId.NUMERICAL){
						id2StringWriter.append(new ImmutableBytesWritable(newId.getContent()), key);
						if (localRowCount % 1000 == 0){
							id2StringWriter.syncFs();
						}
						localRowCount++;
						reporter.incrCounter(NUMERICAL_GROUP, "NonNumericals", 1);
					}
					else{
						reporter.incrCounter(NUMERICAL_GROUP, "Numericals", 1);
					}
					
					//emit (elementID, <tripleID, position>) elements
					while (it.hasNext()){										
						output.collect(newId, it.next());
					}	
			}
	}

}
