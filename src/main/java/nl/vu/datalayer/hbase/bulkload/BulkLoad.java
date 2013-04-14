package nl.vu.datalayer.hbase.bulkload;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.connection.NativeJavaConnection;
import nl.vu.datalayer.hbase.id.BaseId;
import nl.vu.datalayer.hbase.id.DataPair;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * Class representing main entry point for Bulk loading process
 * 
 */

public class BulkLoad extends AbstractPrefixMatchBulkLoad{
	
	/**
	 * Used to load successively the tables containing quads
	 */
	private static HTableInterface currentTable = null;
	
	public BulkLoad(Path input, int inputEstimateSize, String outputPath, String schemaSuffix, boolean onlyTriples, int numberOfSlaveNodes) {
		super(input, inputEstimateSize, outputPath, schemaSuffix, onlyTriples, numberOfSlaveNodes);
	}

	protected void bulkLoadQuadTables(Path convertedTripletsPath) throws Exception {
		//SPOC--------------------
		twoStepTableBulkLoad(convertedTripletsPath, HBPrefixMatchSchema.SPOC, PrefixMatch.PrefixMatchSPOCMapper.class);

		if (rdfUnitType == RDFUnit.QUAD){
			bulkLoadQuadOnlyTables(convertedTripletsPath);
		}
		
		//OSPC---------------------------
		twoStepTableBulkLoad(convertedTripletsPath, HBPrefixMatchSchema.OSPC, PrefixMatch.PrefixMatchOSPCMapper.class);
		
		//POCS---------------------
		twoStepTableBulkLoad(convertedTripletsPath, HBPrefixMatchSchema.POCS, PrefixMatch.PrefixMatchPOCSMapper.class);
	}

	private  void bulkLoadQuadOnlyTables(Path convertedTripletsPath) throws Exception, IOException, InterruptedException, ClassNotFoundException {
		//CSPO----------------------
		twoStepTableBulkLoad(convertedTripletsPath, HBPrefixMatchSchema.CSPO, PrefixMatch.PrefixMatchCSPOMapper.class);
		
		//CPSO-------------------------
		twoStepTableBulkLoad(convertedTripletsPath, HBPrefixMatchSchema.CPSO, PrefixMatch.PrefixMatchCPSOMapper.class);
		
		//OCSP--------------------------
		twoStepTableBulkLoad(convertedTripletsPath, HBPrefixMatchSchema.OCSP, PrefixMatch.PrefixMatchOCSPMapper.class);
	}

	private void twoStepTableBulkLoad(Path convertedTripletsPath, int tableIndex, Class<? extends Mapper> tableMapperClass) throws Exception, IOException, InterruptedException, ClassNotFoundException, TableNotFoundException {
		String tableName = HBPrefixMatchSchema.TABLE_NAMES[tableIndex];
		Path tablePath = new Path(outputPath+"/"+tableName);
		if (!fs.exists(tablePath)) {
			long start;
			start = System.currentTimeMillis();
			Job j9 = createPrefixMatchJob(con, convertedTripletsPath, tablePath, tableMapperClass, tableName);
			j9.waitForCompletion(true);
			long time = System.currentTimeMillis() - start;
			System.out.println("[Time] " + tableName + " finished in: " + time + " ms");

			doTableBulkLoad(tablePath, currentTable, con);
		}
	}

	public  Job createPrefixMatchJob(HBaseConnection con, Path input, Path output, Class<? extends Mapper> cls, String tableName) throws Exception {		
		Configuration conf = new Configuration();
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		conf.setBoolean("mapred.compress.map.output", true);
		
		int childJVMSize = getChildJVMSize(conf);
		
		ShuffleStageOptimizer shuffleOptimizer = new ShuffleStageOptimizer(inputSplitSize,
														HBPrefixMatchSchema.KEY_LENGTH+Bytes.SIZEOF_INT+Bytes.SIZEOF_LONG,
														PrefixMatch.getMapOutputRecordSizeEstimate(),
														childJVMSize);		
		configureShuffle(conf, shuffleOptimizer);
		Job j = new Job(conf);

		j.setJobName(tableName+schemaSuffix);
		j.setJarByClass(BulkLoad.class);
		j.setMapperClass(cls);
		j.setMapOutputKeyClass(ImmutableBytesWritable.class);
		j.setMapOutputValueClass(Put.class);
		j.setInputFormatClass(SequenceFileInputFormat.class);
		j.setOutputFormatClass(HFileOutputFormat.class);

		SequenceFileInputFormat.setInputPaths(j, input);
		HFileOutputFormat.setOutputPath(j, output);

		((NativeJavaConnection)con).getConfiguration().setInt("hbase.rpc.timeout", 0);
		currentTable = con.getTable(tableName+schemaSuffix);

		HFileOutputFormat.configureIncrementalLoad(j, (HTable)currentTable);
		return j;
	}
	
	protected HBPrefixMatchSchema createPrefixMatchSchema() {
		return new HBPrefixMatchSchema(con, schemaSuffix, rdfUnitType==RDFUnit.TRIPLE, 2*numberOfSlaveNodes);
	}

	private Job createResourceToTripleJob(Path input, Path output) throws IOException {
	
		Configuration conf = new Configuration();
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		conf.setBoolean("mapred.compress.map.output", true);
		
		int childJVMSize = getChildJVMSize(conf);
		
		ShuffleStageOptimizer shuffleOptimizer = new ShuffleStageOptimizer(inputSplitSize,
													ResourceToTriple.getMapOutputRecordSizeEstimate(),
													ResourceToTriple.getMapOutputRecordSizeEstimate(),
													childJVMSize);
		configureShuffle(conf, shuffleOptimizer);
		
		Job j = new Job(conf);
		j.setJobName("ResourceToTriple");
		
		int reduceTasks = (int)(1.75*(double)numberOfSlaveNodes*(double)TASK_PER_NODE);
		j.setNumReduceTasks(reduceTasks);
	
		j.setJarByClass(BulkLoad.class);
		j.setMapperClass(ResourceToTriple.ResourceToTripleMapper.class);
		j.setReducerClass(ResourceToTriple.ResourceToTripleReducer.class);
	
		j.setMapOutputKeyClass(BaseId.class);
		j.setMapOutputValueClass(DataPair.class);
		j.setOutputKeyClass(NullWritable.class);
		j.setOutputValueClass(ImmutableBytesWritable.class);
		j.setInputFormatClass(SequenceFileInputFormat.class);
		j.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileInputFormat.setInputPaths(j, input);
		SequenceFileOutputFormat.setOutputPath(j, output);
	
		return j;
	}

	protected void runResourceToTripleJob(Path resourceIds, Path convertedTripletsPath) throws IOException, InterruptedException, ClassNotFoundException {
		if (!fs.exists(convertedTripletsPath)) {
			long start = System.currentTimeMillis();
			Job j2 = createResourceToTripleJob(resourceIds, convertedTripletsPath);
			j2.waitForCompletion(true);
			
			System.out.println("[Time] Second pass finished in: "+(System.currentTimeMillis()-start));
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			if (args.length != 3){
				System.out.println("Usage: bulkLoad <inputPath> <inputSizeEstimate in MB> <outputPath>");
				return;
			}		
			
			Properties prop = new Properties();
			prop.load(new FileInputStream("config.properties"));
			
			String schemaSuffix = prop.getProperty(HBPrefixMatchSchema.SUFFIX_PROPERTY, "");
			boolean onlyTriples = Boolean.parseBoolean(prop.getProperty(HBPrefixMatchSchema.ONLY_TRIPLES_PROPERTY, ""));
			int slaveNodes = Integer.parseInt(prop.getProperty(HBPrefixMatchSchema.NUMBER_OF_SLAVE_NODES_PROPERTY, ""));
			
			AbstractPrefixMatchBulkLoad bulkLoad = new BulkLoad(new Path(args[0]),
									Integer.parseInt(args[1]),
									args[2],
									schemaSuffix,
									onlyTriples,
									slaveNodes);
			
			bulkLoad.run();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
}
