package nl.vu.datalayer.hbase.coprocessor;

import java.io.FileNotFoundException;
import java.io.IOException;

import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.connection.NativeJavaConnection;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

public class LoadCoprocessor {
	
	private static void runCoprocessors(Configuration conf) throws IOException {
		try {
			conf.setInt("hbase.rpc.timeout", 0);
			HTableInterface spoc = new HTable(conf, HBPrefixMatchSchema.TABLE_NAMES[HBPrefixMatchSchema.SPOC]+"_TestCoproc");
		
			spoc.coprocessorExec(PrefixMatchProtocol.class, null, null, 
									Batch.forMethod(PrefixMatchProtocol.class, "generateSecondaryIndex"));
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
		System.out.println("Coprocessors finished");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			if (args.length != 3){
				System.err.println("Usage: LoadCoprocessor <tableName> <coprocessorClass> <jarPath>");
				return;
			}
			NativeJavaConnection con = (NativeJavaConnection)HBaseConnection.create(HBaseConnection.NATIVE_JAVA);
			HBaseAdmin admin = con.getAdmin();
			admin.disableTable(args[0]);
			
			System.out.println("Loading coprocessor from: "+args[1]);
			HTableDescriptor htd = new HTableDescriptor(admin.getTableDescriptor(args[0].getBytes()));
			
			
			//TODO htd.removeCoprocessor(args[1]);
			htd.addCoprocessor(args[1], 
					//new Path("hdfs:///user/sfu200/hbase-0.0.1-SNAPSHOT-jar-with-dependencies.jar"),
					new Path(args[2]),
					Coprocessor.PRIORITY_USER, null);
			
			admin.modifyTable(args[0].getBytes(), htd);
			
			admin.enableTable(args[0].getBytes());

			//runCoprocessors(con.getConfiguration());
			
			admin.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
