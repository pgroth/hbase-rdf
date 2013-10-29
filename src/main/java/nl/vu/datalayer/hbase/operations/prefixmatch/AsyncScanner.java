package nl.vu.datalayer.hbase.operations.prefixmatch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.ScanFilter;
import org.hbase.async.Scanner;

import com.stumbleupon.async.Callback;

public class AsyncScanner {
	
	private Scanner asyncScanner;
	private String tableName;
	private byte []startKey;
	private CountDownLatch latch = null;
	private int scanningStep;
	
	public AsyncScanner(HBaseClient client, String tableName, 
			byte[] startKey, byte []family, byte []qualifier,
			int scanningStep) {
		super();
		this.tableName = tableName;
		this.startKey = startKey;
		this.asyncScanner = client.newScanner(tableName);
		asyncScanner.setFamily(family);
		asyncScanner.setStartKey(startKey);
		asyncScanner.setQualifier(qualifier);
		asyncScanner.setServerBlockCache(false);
		this.scanningStep = scanningStep;
	}
	
	public void start(){
		if (latch==null){
			throw new RuntimeException("Latch should be set before starting the asynchronous scanner");
		}
		
		asyncScanner.nextRows(scanningStep).addCallback(new ScannerCallback());
	}
	
	public void setLatch(CountDownLatch latch){
		this.latch = latch;
	}
	
	public void setFilter(ScanFilter filter){
		this.asyncScanner.setFilter(filter);
	}
	
	class ScannerCallback implements Callback<Boolean, ArrayList<ArrayList<KeyValue>>>{

		public ScannerCallback() {
			super();
		}
		
		@Override
		public Boolean call(ArrayList<ArrayList<KeyValue>> rows) throws Exception {
			if (rows==null){
				latch.countDown();
				return false;
			}
			
			/*for (ArrayList<KeyValue> arrayList : rows) {
				System.out.print("Row: ");
				for (KeyValue keyValue : arrayList) {
					System.out.print(keyValue+" ");
				}
				System.out.println();
			}
			
			latency();*/
			asyncScanner.nextRows(scanningStep).addCallback(new ScannerCallback());
			return true;
		}
		
		/*void latency() throws Exception{
			if (System.currentTimeMillis()%2==0){
				Thread.sleep(1000);
			}
		}*/
	}
}
