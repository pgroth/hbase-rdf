package nl.vu.datalayer.hbase.operations.prefixmatch;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

public class AsyncScannerPool {

	private ArrayList<AsyncScanner> scanners;
	private boolean started=false;
	private CountDownLatch countDownLatch;
	
	public AsyncScannerPool() {
		super();
		scanners = new ArrayList<AsyncScanner>();
	}
	
	public void addNewScanner(AsyncScanner scanner){
		if (started==true){
			throw new RuntimeException("Can not add a new scanner to the pool, scanning already started");
		}
		scanners.add(scanner);
	}
	
	public void doParallelScan() throws InterruptedException{//blocking call which waits for all scans to finish
		started=true;
		countDownLatch = new CountDownLatch(scanners.size());
		
		for (AsyncScanner scanner : scanners) {
			scanner.setLatch(countDownLatch);
			scanner.start();
		}
		
		countDownLatch.await();
	}
	
}
