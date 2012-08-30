package nl.vu.datalayer.hbase.bulkload;

import java.io.IOException;

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
	
	public BulkLoad(Path input, int inputEstimateSize, String outputPath, String schemaSuffix, boolean onlyTriples) {
		super(input, inputEstimateSize, outputPath, schemaSuffix, onlyTriples);
	}

	protected void bulkLoadQuadTables(Path convertedTripletsPath) throws Exception {
		//SPOC--------------------
		twoStepTableBulkLoad(convertedTripletsPath, HBPrefixMatchSchema.SPOC, PrefixMatch.PrefixMatchSPOCMapper.class);

		if (onlyTriples == false){
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

	private  void initializeLocalVariables(String[] args) {
		input = new Path(args[0]);
		inputEstimateSize = Integer.parseInt(args[1]);
		outputPath = args[2];
		schemaSuffix = args[3];
		onlyTriples = Boolean.parseBoolean(args[4]);
	}

	public  Job createPrefixMatchJob(HBaseConnection con, Path input, Path output, Class<? extends Mapper> cls, String tableName) throws Exception {		
		Configuration conf = new Configuration();
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
		ShuffleStageOptimizer shuffleOptimizer = new ShuffleStageOptimizer(inputSplitSize,
														HBPrefixMatchSchema.KEY_LENGTH+Bytes.SIZEOF_INT+Bytes.SIZEOF_LONG,
														PrefixMatch.getMapOutputRecordSizeEstimate());		
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
		return new HBPrefixMatchSchema(con, schemaSuffix);
	}

	private Job createResourceToTripleJob(Path input, Path output) throws IOException {
	
		Configuration conf = new Configuration();
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
		ShuffleStageOptimizer shuffleOptimizer = new ShuffleStageOptimizer(inputSplitSize,
													ResourceToTriple.getMapOutputRecordSizeEstimate(),
													ResourceToTriple.getMapOutputRecordSizeEstimate());
		configureShuffle(conf, shuffleOptimizer);
		
		Job j = new Job(conf);
		j.setJobName("ResourceToTriple");
		
		int reduceTasks = (int)(1.75*(double)CLUSTER_SIZE*(double)TASK_PER_NODE);
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
			if (args.length != 5){
				System.out.println("Usage: bulkLoad <inputPath> <inputSizeEstimate in MB> <outputPath> <schemaSuffix> <onlyTriples(true/false)>");
				return;
			}		
			AbstractPrefixMatchBulkLoad bulkLoad = new BulkLoad(new Path(args[0]),
									Integer.parseInt(args[1]),
									args[2],
									args[3],
									Boolean.parseBoolean(args[4]));
			
			bulkLoad.run();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
}
