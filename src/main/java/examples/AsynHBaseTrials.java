package examples;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

public class AsynHBaseTrials {
	
	static class PrintRows implements Callback<Boolean, ArrayList<ArrayList<KeyValue>>>{
		
		private Scanner scanner;
		private CountDownLatch countDownLatch;

		public PrintRows(Scanner scanner, CountDownLatch countDownLatch) {
			super();
			this.scanner = scanner;
			this.countDownLatch = countDownLatch;
		}
		
		static void latency() throws Exception{
			if (System.currentTimeMillis()%2==0){
				Thread.sleep(1000);
			}
		}


		@Override
		public Boolean call(ArrayList<ArrayList<KeyValue>> rows) throws Exception {
			if (rows==null){
				countDownLatch.countDown();
				return false;
			}
			
			for (ArrayList<KeyValue> arrayList : rows) {
				System.out.print("Row: ");
				for (KeyValue keyValue : arrayList) {
					System.out.print(keyValue+" ");
				}
				System.out.println();
			}
			
			latency();
			scanner.nextRows(2).addCallback(new AsynHBaseTrials.PrintRows(scanner, countDownLatch));
			return true;
		}
		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		HBaseClient asyncClient = new HBaseClient("localhost");
		
		Scanner id2StringScanner = asyncClient.newScanner("Id2String");
		Scanner string2IdScanner = asyncClient.newScanner("String2Id");
		try {
			CountDownLatch countDownLatch = new CountDownLatch(2);
			Deferred<Boolean> worker1 = createWorkersForTable(id2StringScanner, countDownLatch);
			System.out.println("Finished build Id2String workers");
			Deferred<Boolean> worker2 = createWorkersForTable(string2IdScanner, countDownLatch);
			System.out.println("Finished build String2Id workers");
			//Deferred<ArrayList<Object>> workersGroup = Deferred.group(workers);
			
			worker1.joinUninterruptibly();
			worker2.joinUninterruptibly();
			//workersGroup.joinUninterruptibly();
			countDownLatch.await();
			
			id2StringScanner.close().joinUninterruptibly();
			string2IdScanner.close().joinUninterruptibly();
			asyncClient.shutdown().joinUninterruptibly();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	private static Deferred<Boolean> createWorkersForTable(Scanner scanner, CountDownLatch countDownLatch) throws Exception {
		ArrayList<Deferred<Boolean>> workerArray=new ArrayList<Deferred<Boolean>>();
		ArrayList<ArrayList<KeyValue>> res = null;
		int count=0;
		/*while ((res=scanner.nextRows(1).joinUninterruptibly())!=null){
			for (ArrayList<KeyValue> arrayList : res) {
				System.out.print("Row: ");
				for (KeyValue keyValue : arrayList) {
					System.out.print(keyValue+" ");
				}
				System.out.println();
			}
		}*/
		
		Deferred<ArrayList<ArrayList<KeyValue>>> deferredRows=null;
		Deferred<Boolean> worker = null;
		//do{
			deferredRows = scanner.nextRows(2);
			
			worker = deferredRows.addCallback(new AsynHBaseTrials.PrintRows(scanner, countDownLatch));
			workerArray.add(worker);
			System.out.println("Count: "+(count++));
			if (count>691){
				System.out.println("Above");
			}
		//}
		//while (worker.joinUninterruptibly()==true);
		
			
		return worker;
	}

}
