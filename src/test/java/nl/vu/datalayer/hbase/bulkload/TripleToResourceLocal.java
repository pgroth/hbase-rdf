package nl.vu.datalayer.hbase.bulkload;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@Deprecated
public class TripleToResourceLocal extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {

		/*Path resourceIds = new Path("./" + TripleToResource.RESOURCE_IDS_DIR);
		Job j1 = BulkLoad.createTripleToResourceJob(new Path("input"), resourceIds, 100);
		j1.getConfiguration().set("outputPath", "./");
		j1.waitForCompletion(true);

		/*
		 * JobConf j1 = BulkLoad.createTripleToResourceJob(new Path("input"),
		 * resourceIds, 2); j1.set("outputPath", "./"); RunningJob runningJ1 =
		 * JobClient.runJob(j1);
		 
		BulkLoad.retrieveTripleToResourceCounters(j1);

		Path convertedTripletsPath = new Path("./" + ResourceToTriple.TEMP_TRIPLETS_DIR);
		Job j2 = BulkLoad.createResourceToTripleJob(resourceIds, convertedTripletsPath);
		j2.waitForCompletion(true);

		/*
		 * Path convertedTripletsPath = new Path("tempTriplets"); Job j2 =
		 * BulkLoad.createResourceToTripleJob(resourceIds,
		 * convertedTripletsPath); j2.waitForCompletion(true);
		 */

		// Path id2String = new Path("id2String");
		// JobConf j2 = BulkLoadNew.createResourceToTripleJob(resourceIds,
		// id2String);
		// JobClient.runJob(j2);

		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int ret;
		try {
			ret = ToolRunner.run(new TripleToResourceLocal(), args);
			System.exit(ret);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
