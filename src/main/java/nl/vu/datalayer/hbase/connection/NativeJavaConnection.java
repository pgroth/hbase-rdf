package nl.vu.datalayer.hbase.connection;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.PoolMap;

public class NativeJavaConnection extends HBaseConnection {
	
	protected HBaseAdmin hbase = null;
	protected Configuration conf = null;
	private HTablePool tablePool = null;
	
	public static final int MAX_POOL_SIZE = 20;
	
	public NativeJavaConnection() throws MasterNotRunningException, ZooKeeperConnectionException{
		//add the "hbase-site.xml" file to the classpath to get the bootstrapping Zookeeper nodes
		conf = HBaseConfiguration.create();
		tablePool = new HTablePool(conf, MAX_POOL_SIZE, PoolMap.PoolType.Reusable);
		hbase = new HBaseAdmin(conf);
	}
	
	public void initTables(String []tableNames) throws IOException{
		int iterations = MAX_POOL_SIZE/tableNames.length;
		HTable [][]tables = new HTable[iterations][];
		
		//open tables
		for (int i = 0; i < iterations; i++) {
			tables[i] = new HTable[tableNames.length];
			for (int j = 0; j < tableNames.length; j++) {
				if (hbase.tableExists(tableNames[j])){
					tables[i][j] = (HTable)tablePool.getTable(tableNames[j]);
					tables[i][j].prewarmRegionCache(tables[i][j].getRegionsInfo());
				}
				else{ 
					tables[i][j] = null;
				}
			}
		}
		
		//close tables
		for (int i = 0; i < tables.length; i++) {
			for (int j = 0; j < tables.length; j++) {
				if (tables[i][j]!=null){
					tables[i][j].close();
				}
			}
		}
	}
	
	public HTableInterface getTable(String tableName) throws IOException{
		return tablePool.getTable(tableName);
	}
	
	public HTableInterface getTable(byte [] tableNameBytes) throws IOException{
		return tablePool.getTable(tableNameBytes);
	}
	
	public void close() throws IOException{
		hbase.close();
		tablePool.close();
	}
	
	public HBaseAdmin getAdmin() {
		return hbase;
	}
	
	public Configuration getConfiguration() {
		return conf;
	}

}
