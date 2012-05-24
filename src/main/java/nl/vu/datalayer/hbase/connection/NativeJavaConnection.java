package nl.vu.datalayer.hbase.connection;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;

public class NativeJavaConnection extends HBaseConnection {
	
	protected HBaseAdmin hbase = null;
	protected Configuration conf = null;
	
	public NativeJavaConnection() throws MasterNotRunningException, ZooKeeperConnectionException{
		//add the "hbase-site.xml" file to the classpath to get the bootstrapping Zookeeper nodes
		conf = HBaseConfiguration.create();
		hbase = new HBaseAdmin(conf);
	}
	
	public HTableInterface getTable(String tableName) throws IOException{
		return new HTable(conf, tableName);
	}
	
	public void close() throws IOException{
		hbase.close();
	}
	
	public HBaseAdmin getAdmin() {
		return hbase;
	}
	
	public Configuration getConfiguration() {
		return conf;
	}

}
