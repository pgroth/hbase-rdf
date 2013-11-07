package nl.vu.datalayer.hbase;

import java.io.IOException;
import java.util.List;

import nl.vu.datalayer.hbase.connection.HBaseConnection;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import com.sematext.hbase.wd.AbstractRowKeyDistributor;
import com.sematext.hbase.wd.DistributedScanner;
import com.sematext.hbase.wd.RowKeyDistributorByHashPrefix;
import com.sematext.hbase.wd.RowKeyDistributorByOneBytePrefix;

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
		HTableInterface testTable = con.getTable("JOIN");
		
		byte bucketsCount = 8; // distributing into 32 buckets
		AbstractRowKeyDistributor keyDistributor =
				new RowKeyDistributorByOneBytePrefix(bucketsCount);
	    /*for (int i = 0; i < 21; i++) {
	      Put put = new Put(keyDistributor.getDistributedKey(Bytes.toBytes(i)));
	      put.add("B".getBytes(), "Q".getBytes(), null);
	      testTable.put(put);
	    }*/
	    
		byte []dummy=new byte[0];
	    
	    
	    FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
	    SingleColumnValueFilter colfilter2 = new SingleColumnValueFilter("J".getBytes(), new byte[]{0x01, 0x01}, CompareOp.NOT_EQUAL, dummy);
		colfilter2.setFilterIfMissing(true);
		filters.addFilter(colfilter2);
		
	    SingleColumnValueFilter colfilter1 = new SingleColumnValueFilter("J".getBytes(), new byte[]{0x00, 0x00}, CompareOp.NOT_EQUAL, dummy);
		colfilter1.setFilterIfMissing(true);
		filters.addFilter(colfilter1);
		
		SingleColumnValueFilter colfilter3 = new SingleColumnValueFilter("J".getBytes(), new byte[]{0x02, 0x02}, CompareOp.NOT_EQUAL, dummy);
		colfilter3.setFilterIfMissing(true);
		filters.addFilter(colfilter3);
		SingleColumnValueFilter colfilter4 = new SingleColumnValueFilter("J".getBytes(), new byte[]{0x03}, CompareOp.NOT_EQUAL, dummy);
		colfilter4.setFilterIfMissing(true);
		filters.addFilter(colfilter4);
		
		Scan scan = new Scan(new byte[]{0x00,0x00, 0x00}, new byte[]{0x09, 0x00, 0x01});
		scan.setFilter(filters);
		
		//ResultScanner rs = testTable.getScanner(scan);
	    ResultScanner rs = DistributedScanner.create(testTable, scan, keyDistributor);
	    
	    Result current=null;
	    while ((current=rs.next())!=null){
	    	System.out.println(current);
	    }
	}

	private static void testScanWithRequiredColumns(HBaseConnection con) throws IOException {
		HTableInterface testTable = con.getTable("FilterTest");
		
		byte []dummy=new byte[0];
		FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		
		SingleColumnValueFilter colfilter0 = new SingleColumnValueFilter("F".getBytes(), new byte[]{0x03, 0x02}, CompareOp.NOT_EQUAL, dummy);
		colfilter0.setFilterIfMissing(true);
		filters.addFilter(colfilter0);
		
		SingleColumnValueFilter colfilter1 = new SingleColumnValueFilter("F".getBytes(), "q1".getBytes(), CompareOp.NOT_EQUAL, dummy);
		colfilter1.setFilterIfMissing(true);
		filters.addFilter(colfilter1);
		SingleColumnValueFilter colfilter2 = new SingleColumnValueFilter("F".getBytes(), "q2".getBytes(), CompareOp.NOT_EQUAL, dummy);
		colfilter2.setFilterIfMissing(true);
		filters.addFilter(colfilter2);
		SingleColumnValueFilter colfilter3 = new SingleColumnValueFilter("F".getBytes(), "q3".getBytes(), CompareOp.NOT_EQUAL, dummy);
		colfilter3.setFilterIfMissing(true);
		filters.addFilter(colfilter3);
		
		Scan scan = new Scan("ro".getBytes(), "w".getBytes());
		scan.setFilter(filters);
		
		ResultScanner results = testTable.getScanner(scan);
		
		Result row=null;
		System.out.println("Starting scan..");
		while ((row=results.next())!=null){
				System.out.println(row);				
		}
		
		results.close();
		testTable.close();
	}

}
