package nl.vu.datalayer.hbase;

import java.io.IOException;
import java.util.List;

import nl.vu.datalayer.hbase.connection.HBaseConnection;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

public class HBaseTests {
	
	public static void main(String[] args) {
		try {
			HBaseConnection con = HBaseConnection.create(HBaseConnection.NATIVE_JAVA);
			
			HTableInterface testTable = con.getTable("FilterTest");
			
			byte []dummy=new byte[0];
			SingleColumnValueFilter colfilter1 = new SingleColumnValueFilter("F".getBytes(), "q1".getBytes(), CompareOp.NOT_EQUAL, dummy);
			colfilter1.setFilterIfMissing(true);
			SingleColumnValueFilter colfilter2 = new SingleColumnValueFilter("F".getBytes(), "q2".getBytes(), CompareOp.NOT_EQUAL, dummy);
			colfilter2.setFilterIfMissing(true);
			SingleColumnValueFilter colfilter3 = new SingleColumnValueFilter("F".getBytes(), "q3".getBytes(), CompareOp.NOT_EQUAL, dummy);
			colfilter3.setFilterIfMissing(true);
			
			FilterList filters = new FilterList(colfilter1, colfilter2, colfilter3);
			
			Scan scan = new Scan(dummy, filters);
			scan.setMaxVersions(2);
			ResultScanner results = testTable.getScanner(scan);
			
			Result row=null;
			System.out.println("Starting scan..");
			while ((row=results.next())!=null){
				List<KeyValue> keyValues = row.getColumn("F".getBytes(), "q2".getBytes());
				for (KeyValue keyValue : keyValues) {
					System.out.println(keyValue);
				}				
			}
			
			results.close();
			testTable.close();
			con.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
