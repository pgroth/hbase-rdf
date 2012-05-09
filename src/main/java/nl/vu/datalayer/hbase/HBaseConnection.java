package nl.vu.datalayer.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HBaseConnection {

	protected HBaseAdmin hbase = null;
	protected Configuration conf = null;

	public static final String ZOOKEEPER_QUORUM = "das3001.cm.cluster,das3002.cm.cluster,das3003.cm.cluster,das3004.cm.cluster,das3005.cm.cluster";

	public HBaseConnection() throws Exception {
		conf = HBaseConfiguration.create();

		// conf.set("hbase.master","fs0.cm.cluster:60000");
		conf.set("hbase.master", "fs0.das4.cs.vu.nl:60010");
		conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);

		System.out.println(conf);
		hbase = new HBaseAdmin(conf);
	}

	public HBaseAdmin getAdmin() {
		return hbase;
	}

	public Configuration getConfiguration() {
		return conf;
	}

	public void close() throws IOException {
		hbase.close();
	}

}
