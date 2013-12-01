package nl.vu.datalayer.hbase.connection;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.hbase.async.HBaseClient;

/**
 * Inherits synchronous methods, but also adds access through the async client
 * 
 * @author Sever
 *
 */
public class AsyncNativeJavaConnection extends NativeJavaConnection {

	public final String QUORUM_PROP = "hbase.zookeeper.quorum";
	private HBaseClient asyncClient;
	
	public AsyncNativeJavaConnection() throws MasterNotRunningException, ZooKeeperConnectionException {
		super();
		
		String zookeeperQuorum = hbase.getConfiguration().get(QUORUM_PROP);
		asyncClient = new HBaseClient(zookeeperQuorum);
	}

	public HBaseClient getAsyncClient() {
		return asyncClient;
	}

}
