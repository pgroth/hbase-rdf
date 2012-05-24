package nl.vu.datalayer.hbase.test;

import java.io.IOException;
import java.util.ArrayList;

import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.HBaseFactory;
import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;
import nl.vu.datalayer.hbase.util.IHBaseUtil;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

public class HBaseLocalPrefixMatchTest {
	
	public static void main(String[] args) {
		try {
			/*String outputPath = "./";
			
			HBaseConnection con = new HBaseConnection();
			HBPrefixMatchSchema.createCounterTable(con.getAdmin());
			Path resourceIds = new Path(outputPath+TripleToResource.RESOURCE_IDS_DIR);
			
			Job j1 = BulkLoad.createTripleToResourceJob(new Path("input"), resourceIds, 100);
			j1.getConfiguration().set("outputPath", outputPath);
			j1.waitForCompletion(true);
			
			BulkLoad.retrieveCounters(j1);
			
			Path convertedTripletsPath = new Path("./"+ResourceToTriple.TEMP_TRIPLETS_DIR);
			Job j2 = BulkLoad.createResourceToTripleJob(resourceIds, convertedTripletsPath);	
			j2.waitForCompletion(true);
			
			long startPartition = HBPrefixMatchSchema.updateLastCounter(BulkLoad.tripleToResourceReduceTasks, con.getConfiguration())+1;
			*/
			HBaseConnection con = HBaseConnection.create(HBaseConnection.NATIVE_JAVA);
			HBaseClientSolution sol = HBaseFactory.getHBaseSolution(HBPrefixMatchSchema.SCHEMA_NAME, con, null);	
			//long totalStringCount = 3000L;
			//long numericalCount = 500L;
			//int outPartitions = 4;
			//BulkLoad.buildCountersFromFile();
			//((HBPrefixMatchSchema)sol.schema).setTableSplitInfo(BulkLoad.totalStringCount, BulkLoad.numericalCount, 
				//	BulkLoad.tripleToResourceReduceTasks, 0, BulkLoad.sufixCounters);	
			//sol.schema.create();			
			
			/*LoadIncrementalHFiles bulkLoad = new LoadIncrementalHFiles(con.getConfiguration());
			
			//String2Id PASS----------------------------------------------
					
			Path string2IdInput = new Path(outputPath+TripleToResource.ID2STRING_DIR);
			Path string2IdOutput = new Path(outputPath+StringIdAssoc.STRING2ID_DIR);
			Job j3 = BulkLoad.createString2IdJob(con, string2IdInput, string2IdOutput);
			j3.waitForCompletion(true);
			
			//string2Id = new HTable(con.getConfiguration(), HBPrefixMatchSchema.STRING2ID);
			BulkLoad.doBulkLoad(bulkLoad, string2IdOutput, BulkLoad.string2Id);
			
			System.out.println("Finished bulk load for String2Id table");//=====================//=============
			
			Path id2StringOutput = new Path(outputPath+StringIdAssoc.ID2STRING_DIR);
			Job j4 = BulkLoad.createId2StringJob(con, string2IdInput, id2StringOutput);
			j4.waitForCompletion(true);
			
			BulkLoad.doBulkLoad(bulkLoad, id2StringOutput, BulkLoad.id2String);
			
			//====================
			
			Path spocPath = new Path(outputPath+"/"+HBPrefixMatchSchema.TABLE_NAMES[HBPrefixMatchSchema.SPOC]);
			
			Job j5 = BulkLoad.createPrefixMatchJob(con, convertedTripletsPath, spocPath, HBPrefixMatchSchema.SPOC, PrefixMatch.PrefixMatchSPOCMapper.class);
			j5.waitForCompletion(true);
			
			BulkLoad.doBulkLoad(bulkLoad, spocPath, BulkLoad.currentTable);
			
			//setObjectPrefixTableSplitInfo(BulkLoad.totalStringCount, BulkLoad.numericalCount);
			//((HBPrefixMatchSchema)sol.schema).setId2StringTableSplitInfo(BulkLoad.tripleToResourceReduceTasks, startPartition);
			
			//SortedMap<Short, Long> prefixCounterMap = new TreeMap();
			//prefixCounterMap.put((short)'a', 1000L);
			//prefixCounterMap.put((short)'b', 2000L);
			//((HBPrefixMatchSchema)sol.schema).setString2IdTableSplitInfo(BulkLoad.totalStringCount, BulkLoad.sufixCounters);
			*/
			
			//HBaseConnection con = new HBaseConnection();
			//HBaseClientSolution sol = HBaseFactory.getHBaseSolution(HBPrefixMatchSchema.SCHEMA_NAME, con, null);	
			IHBaseUtil util = sol.util;
			String []quad = new String[4];
			quad[0] = "aa";
			quad[1] = "?";
			quad[2] = "?";
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
