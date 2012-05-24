package nl.vu.datalayer.hbase.connection;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import nl.vu.datalayer.hbase.schema.PrefixMatchRemoteHTable;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;

public class RESTConnection extends HBaseConnection {
	
	public static final String REST_SERVER = "rest_server";
	public static final String REST_PORT = "rest_port";
	
	public static final String DAS4_SERVER = "fs0.das4.cs.vu.nl";
	public static final String DAS4_PORT = "8090";
	
	private Client client = null;

	public RESTConnection() throws IOException {
		super();
		Cluster cluster = new Cluster();
		
		Properties prop = new Properties();
		try{
			prop.load(new FileInputStream("config.properties"));
		}
		catch (FileNotFoundException e) {
			System.err.println("[WARN] Config file not found");
		}
		String restServer = prop.getProperty(REST_SERVER, DAS4_SERVER);
		int restPort = Integer.parseInt(prop.getProperty(REST_PORT, DAS4_PORT));		
		cluster.add(restServer, restPort);
		client = new Client(cluster);
	}
	
	public HTableInterface getTable(String tableName) throws IOException{
		return new PrefixMatchRemoteHTable(client, tableName);
	}

}
