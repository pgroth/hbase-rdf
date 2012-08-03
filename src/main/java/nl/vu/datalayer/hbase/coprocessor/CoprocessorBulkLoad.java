package nl.vu.datalayer.hbase.coprocessor;

import java.io.IOException;
import java.util.List;

import nl.vu.datalayer.hbase.bulkload.AbstractPrefixMatchBulkLoad;
import nl.vu.datalayer.hbase.bulkload.BulkLoad;
import nl.vu.datalayer.hbase.bulkload.ResourceToTriple;
import nl.vu.datalayer.hbase.bulkload.ShuffleStageOptimizer;
import nl.vu.datalayer.hbase.id.BaseId;
import nl.vu.datalayer.hbase.id.DataPair;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class CoprocessorBulkLoad extends AbstractPrefixMatchBulkLoad {

	private String coprocessorPath;
	
	public CoprocessorBulkLoad(Path input, int inputEstimateSize, String outputPath, 
								String schemaSuffix, boolean onlyTriples,
								String coprocessorPath) {
		super(input, inputEstimateSize, outputPath, schemaSuffix, onlyTriples);
		this.coprocessorPath = coprocessorPath;
	}

	@Override
	protected void bulkLoadQuadTables(Path convertedTripletsPath) throws Exception {
		//do nothing
	}
	
	protected HBPrefixMatchSchema createPrefixMatchSchema() {
		return new HBPrefixMatchSchema(con, schemaSuffix, coprocessorPath);
	}

	private Job createResourceToTripleJob(Path input, Path output) throws IOException {
		Configuration conf = new Configuration();
		
		ShuffleStageOptimizer shuffleOptimizer = new ShuffleStageOptimizer(inputSplitSize,
				ResourceToTriple.getMapOutputRecordSizeEstimate(),
				ResourceToTriple.getMapOutputRecordSizeEstimate());
		configureShuffle(conf, shuffleOptimizer);
		
		conf.set("SUFFIX", schemaSuffix);
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
		Job j = new Job(conf);
		j.setJobName("TableInsertResourceToTriple");
		
		int reduceTasks = (int)(1.75*(double)CLUSTER_SIZE*(double)TASK_PER_NODE);
		j.setNumReduceTasks(reduceTasks);
	
		j.setJarByClass(BulkLoad.class);
		j.setMapperClass(ResourceToTriple.ResourceToTripleMapper.class);
		j.setReducerClass(TableInsertResourceToTriple.class);
	
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
		if (!fs.exists(convertedTripletsPath)){
			long start = System.currentTimeMillis();
			Job j2 = createResourceToTripleJob(resourceIds, convertedTripletsPath);
			j2.waitForCompletion(true);
			long j2Time = System.currentTimeMillis() - start;
			System.out.println("[Time] Second pass finished: " + j2Time + " ms ; " + ((double) j2Time / 60.0) + " min");
		}
		runCoprocessors();
		//flushCoprocessorBuffers((HBPrefixMatchSchema.TABLE_NAMES[HBPrefixMatchSchema.SPOC]+schemaSuffix).getBytes());
	}

	private void runCoprocessors() throws IOException {
		long start = System.currentTimeMillis();
		con.getConfiguration().setInt("hbase.rpc.timeout", 0);
		HTable spoc = (HTable)con.getTable(HBPrefixMatchSchema.TABLE_NAMES[HBPrefixMatchSchema.SPOC]+schemaSuffix);
		spoc.setOperationTimeout(0);
		
		try {
			spoc.coprocessorExec(PrefixMatchProtocol.class, null, null, 
									Batch.forMethod(PrefixMatchProtocol.class, "generateSecondaryIndex"));
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
		long coprocLoadingTime = System.currentTimeMillis()-start;
		System.out.println("[Time] Coprocessor loading finished in: "+coprocLoadingTime+" ms");
	}

	public static void main(String[] args) {
		try {
			if (args.length != 6){
				System.out.println("Usage: bulkLoad <inputPath> <inputSizeEstimate in MB> <outputPath> "
										+"<schemaSuffix> <onlyTriples(true/false)> <coprocessorPath>");
				return;
			}		
			CoprocessorBulkLoad bulkLoad = new CoprocessorBulkLoad(new Path(args[0]),
									Integer.parseInt(args[1]),
									args[2],
									args[3],
									Boolean.parseBoolean(args[4]),
									args[5]);
			
			bulkLoad.run();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	
}
