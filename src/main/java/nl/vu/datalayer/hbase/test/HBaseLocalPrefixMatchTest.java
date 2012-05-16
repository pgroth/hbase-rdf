package nl.vu.datalayer.hbase.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.HBaseConnection;
import nl.vu.datalayer.hbase.HBaseFactory;
import nl.vu.datalayer.hbase.bulkload.BulkLoad;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;
import nl.vu.datalayer.hbase.util.IHBaseUtil;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

public class HBaseLocalPrefixMatchTest {
	
	public static void main(String[] args) {
		try {
			HBaseConnection con = new HBaseConnection();
			HBPrefixMatchSchema.createCounterTable(con.getAdmin());
			
			Path resourceIds = new Path("resourceIds");
			JobConf j1 = BulkLoad.createTripleToResourceJob(new Path("input"), resourceIds, 2);
			j1.set("outputPath", "./");
			RunningJob runningJ1 = JobClient.runJob(j1);
			
			BulkLoad.retrieveCounters(runningJ1);
			
			long startPartition = HBPrefixMatchSchema.updateLastCounter(BulkLoad.tripleToResourceReduceTasks, con.getConfiguration())+1;
			
			HBaseClientSolution sol = HBaseFactory.getHBaseSolution(HBPrefixMatchSchema.SCHEMA_NAME, con, null);	
			//long totalStringCount = 3000L;
			//long numericalCount = 500L;
			//int outPartitions = 4;
			//BulkLoad.buildCountersFromFile();
		
			
			((HBPrefixMatchSchema)sol.schema).setObjectPrefixTableSplitInfo(BulkLoad.totalStringCount, BulkLoad.numericalCount);
			((HBPrefixMatchSchema)sol.schema).setId2StringTableSplitInfo(BulkLoad.tripleToResourceReduceTasks, startPartition);
			
			//SortedMap<Short, Long> prefixCounterMap = new TreeMap();
			//prefixCounterMap.put((short)'a', 1000L);
			//prefixCounterMap.put((short)'b', 2000L);
			((HBPrefixMatchSchema)sol.schema).setString2IdTableSplitInfo(BulkLoad.totalStringCount, BulkLoad.sufixCounters);
			sol.schema.create();
			
			IHBaseUtil util = sol.util;
			String []quad = new String[4];
			quad[0] = "?";
			quad[1] = "a";
			quad[2] = "c";
			quad[3] = "?";
			
			ArrayList<ArrayList<String>> results = util.getRow(quad);
			
			for (ArrayList<String> arrayList : results) {
				for (String string : arrayList) {
					System.out.print(string+" ");
				}
				System.out.println();
			}
				
			con.close();
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
}
