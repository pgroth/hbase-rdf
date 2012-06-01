package nl.vu.datalayer.hbase.connection;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;


public abstract class HBaseConnection {

	public static final byte NATIVE_JAVA = 0;
	public static final byte REST = 1;
	
	public static HBaseConnection create(byte connectionType) throws IOException{
		switch (connectionType){
		case NATIVE_JAVA: {	
			return new NativeJavaConnection();
		}
		case REST:{
			return new RESTConnection();
		}
		default:
			throw new RuntimeException("Unknow HBase connection type");
		}
	}
	
	public abstract HTableInterface getTable(String tableName) throws IOException;
	
	public void close() throws IOException{}
}
