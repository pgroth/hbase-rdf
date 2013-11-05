package nl.vu.datalayer.hbase;

import java.io.IOException;
import java.util.List;

import nl.vu.datalayer.hbase.connection.HBaseConnection;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.sematext.hbase.wd.AbstractRowKeyDistributor;
import com.sematext.hbase.wd.DistributedScanner;
import com.sematext.hbase.wd.RowKeyDistributorByHashPrefix;

public class HBaseTests {
	
	public static void main(String[] args) {
		try {
			HBaseConnection con = HBaseConnection.create(HBaseConnection.NATIVE_JAVA);
			
			//testScanWithRequiredColumns(con);
			testBucketedWrites(con);
			con.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void testBucketedWrites(HBaseConnection con) throws IOException{
		HTableInterface testTable = con.getTable("BucketTest");
		
		int bucketsCount = 10; // distributing into 32 buckets
		AbstractRowKeyDistributor keyDistributor =
				new RowKeyDistributorByHashPrefix(new RowKeyDistributorByHashPrefix.OneByteSimpleHash(bucketsCount));
	    /*for (int i = 0; i < 21; i++) {
	      Put put = new Put(keyDistributor.getDistributedKey(Bytes.toBytes(i)));
	      put.add("B".getBytes(), "Q".getBytes(), null);
	      testTable.put(put);
	    }*/
	    
	    Scan scan = new Scan(Bytes.toBytes(0), Bytes.toBytes(21));
	    ResultScanner rs = DistributedScanner.create(testTable, scan, keyDistributor);
	    
	    Result current;
	    while ((current=rs.next())!=null){
	    	System.out.println(current);
	    }
	}

	private static void testScanWithRequiredColumns(HBaseConnection con) throws IOException {
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
	}

}
