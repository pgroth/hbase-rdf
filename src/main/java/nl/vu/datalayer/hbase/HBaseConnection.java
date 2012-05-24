package nl.vu.datalayer.hbase;

import java.io.IOException;

import nl.vu.datalayer.hbase.schema.PrefixMatchRemoteHTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;

/**
 * Deprecated - use the classes from nl.vu.datalayer.hbase.connection instead
 *
 */
@Deprecated 
public class HBaseConnection {
	
	protected HBaseAdmin hbase = null;
	protected Configuration conf = null;
	
	private Client client = null;
	private byte connectionType;
	
	//public static final String ZOOKEEPER_QUORUM = "das3001.cm.cluster,das3002.cm.cluster,das3003.cm.cluster,das3004.cm.cluster,das3005.cm.cluster";
	
	public static final byte NATIVE_JAVA = 0;
	public static final byte REST = 1;
	
	public static final String REST_SERVER = "fs0.das4.cs.vu.nl";
	public static final int SERVER_PORT = 8090;

	public HBaseConnection(byte connectionType) throws MasterNotRunningException, ZooKeeperConnectionException  {
		this.connectionType = connectionType;
		switch (connectionType){
		case NATIVE_JAVA: {	
			//add the "hbase-site.xml" file to the classpath to get the bootstrapping Zookeeper nodes
			conf = HBaseConfiguration.create();
			//conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
			
			System.out.println(conf);
			hbase = new HBaseAdmin(conf);
			break;
		}
		case REST:{
			Cluster cluster = new Cluster();
			cluster.add(REST_SERVER, SERVER_PORT);
			client = new Client(cluster);
			break;
		}
		default:
			throw new RuntimeException("Unknow HBase connection type");
		}
	}
	
	public HBaseAdmin getAdmin() {
		return hbase;
	}
	
	public Configuration getConfiguration() {
		return conf;
	}
	
	public void close() throws IOException{
		if (connectionType == NATIVE_JAVA)
			hbase.close();	
	}
	
	public HTableInterface getTable(String tableName) throws IOException{
		switch(connectionType){
		case NATIVE_JAVA:{
			return new HTable(conf, tableName);
		}
		case REST:{
			return new PrefixMatchRemoteHTable(client, tableName);
		}
		default:
			throw new RuntimeException("Unknow HBase connection type");
		}
	}
	
	/*public Get createGet(byte []rowKey) throws URIException, UnsupportedEncodingException{
		switch(connectionType){
		case JAVA_API:{
			return new Get(rowKey);
		}
		case REST:{
			return new Get(rowKey);
		}
		default:
			throw new RuntimeException("Unknow HBase connection type");
		}
	}
	
	public Scan createScan(byte []rowKey, Filter filter) throws URIException{
		switch(connectionType){
		case JAVA_API:{
			return new Scan(rowKey, filter);
		}
		case REST:{
			//String encoded = URIUtil.encodeAll(Bytes.toString(rowKey));
			return new Scan(rowKey, filter);
		}
		default:
			throw new RuntimeException("Unknow HBase connection type");
		}
	}*/
	
}
