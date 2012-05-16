package nl.vu.datalayer.hbase.schema;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Map;
import java.util.SortedMap;

import nl.vu.datalayer.hbase.HBaseConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

public class HBPrefixMatchSchema implements IHBaseSchema {
	
	private HBaseConnection con;	
	
	public static final String SCHEMA_NAME = "prefix-match";

	public static final byte [] COLUMN_FAMILY = "F".getBytes();
	public static final byte [] COLUMN_NAME = "".getBytes();
	
	public static final String []TABLE_NAMES = {"SPOC", "POCS", "OCSP", "CSPO", "CPSO", "OSPC"};
	public static final String []TEST_TABLE_NAMES = {"SPOCTest", "POCSTest", "OCSPTest", "CSPOTest", "CPSOTest", "OSPCTest"};
	
	public static final int KEY_LENGTH = 33;
	
	//mapping between the SPOC order and the order in other tables
	public static final int [][]ORDER = {{0,1,2,3}, {3,0,1,2}, {2,3,0,1}, {1,2,3,0}, {2,1,3,0}, {1,2,0,3}};
	public static final int [][]OFFSETS = {{0,8,16,25}, {25,0,8,16}, {16,25,0,8}, {8,16,24,0}, {16,8,24,0}, {9,17,0,25}};
	
	public static final int SPOC = 0;
	public static final int POCS = 1;
	public static final int OCSP = 2;
	public static final int CSPO = 3;
	public static final int CPSO = 4;
	public static final int OSPC = 5;
	
	public static final String STRING2ID = "String2Id";
	public static final String ID2STRING = "Id2String";
	
	public static final String COUNTER_TABLE = "Counters";
	public static final byte []META_COL = "Meta".getBytes();
	public static final byte [] FIRST_COUNTER_ROWKEY = "FirstCounter".getBytes();
	public static final byte [] LAST_COUNTER_ROWKEY = "LastCounter".getBytes();
	public static final long COUNTER_LIMIT = 0x000000ffffffffffL;
	
	public static final String STRING2ID_TEST = "String2IdTest";
	public static final String ID2STRING_TEST = "Id2StringTest";
	
	public static final int NUM_REGIONS = 64;
	
	private static int numInputPartitions;
	private static long startPartition;
	
	private static SortedMap<Short, Long> prefixCounters = null;
	//private long bNodeCounters = 0;
	//private long nonNumericalLiteralCounter = 0;
	private static long totalStringCount = 0;
	private static long numericalCount = 0;
	
	public HBPrefixMatchSchema(HBaseConnection con) {
		super();
		this.con = con;
	}
	
	public static void setId2StringTableSplitInfo(int numPartitions, long startPartitionParam){
		startPartition = startPartitionParam;
		numInputPartitions = numPartitions;
	}
	
	public static void setString2IdTableSplitInfo(long totalNumberOfString, SortedMap<Short, Long> prefixCounter){
		totalStringCount = totalNumberOfString;
		prefixCounters = prefixCounter;
		//this.nonNumericalLiteralCounter = nonNummericalLiteralCounter;
		//this.bNodeCounters = bNodeCounters;
	}
	
	public static void setObjectPrefixTableSplitInfo(long totalNumberOfString, long numericalParam){
		numericalCount = numericalParam;
		totalStringCount = totalNumberOfString;
	}
	
	public static void createTable(HBaseAdmin admin, String tableName, byte [][]splits, boolean enableCompression) throws IOException{
		if (admin.tableExists(tableName) == false){
			HTableDescriptor desc = new HTableDescriptor(tableName);

			HColumnDescriptor famDesc = new HColumnDescriptor(COLUMN_FAMILY);
			famDesc.setBloomFilterType(StoreFile.BloomType.ROW);
			famDesc.setMaxVersions(1);
			if (enableCompression)//by default it is disabled
				famDesc.setCompactionCompressionType(Algorithm.LZO);
			desc.addFamily(famDesc);

			System.out.println("Creating table: " + tableName);

			admin.createTable(desc, splits);
		}
	}
	
	public static void createCounterTable(HBaseAdmin admin) throws IOException{
		
		if (admin.tableExists(COUNTER_TABLE) == false){
			HTableDescriptor desc = new HTableDescriptor(COUNTER_TABLE);

			HColumnDescriptor famDesc = new HColumnDescriptor(COLUMN_FAMILY);
			famDesc.setMaxVersions(1);
			desc.addFamily(famDesc);

			System.out.println("Creating table: " + COUNTER_TABLE);
			admin.createTable(desc);
			
			//initialize the values for the first and last counter names
			Put p = new Put(FIRST_COUNTER_ROWKEY);
			p.add(COLUMN_FAMILY, META_COL, Bytes.toBytes(0L));
			
			HTable h = new HTable(COUNTER_TABLE);
			h.put(p);
			
			p = new Put(LAST_COUNTER_ROWKEY);
			p.add(COLUMN_FAMILY, META_COL, Bytes.toBytes(-1L));
			h.put(p);
		}
	}
	
	public static void updateCounter(int partitionId, long localRowCount) throws IOException{
		Configuration conf = HBaseConfiguration.create();		
		long lastCounter = getLastCounter(conf);
		long newCounterColumn = lastCounter+partitionId+1;
		
		HTable h = new HTable(conf, COUNTER_TABLE);
		Put p = new Put(new byte[0]);
		p.add(COLUMN_FAMILY, Bytes.toBytes(newCounterColumn), Bytes.toBytes(localRowCount));
		h.put(p);
		h.close();
	}
	
	/**
	 * @param numCounter
	 * @param conf
	 * @return the last counter column name before adding numCounters
	 * @throws IOException
	 */
	public static Long updateLastCounter(int numCounters, Configuration conf) throws IOException{
		long val = getLastCounter(conf);
		
		if (COUNTER_LIMIT - val < numCounters){
			System.err.println(numCounters+" + "+val+" exceeds counter limit:");
			return null;
		}
		
		HTable h = new HTable(conf, COUNTER_TABLE);
		Put p = new Put(LAST_COUNTER_ROWKEY);
		p.add(COLUMN_FAMILY, META_COL, Bytes.toBytes(val+numCounters));
		h.put(p);
		h.close();
		
		return val;
	}
	
	/**
	 * @param conf
	 * @return
	 * @throws IOException
	 */
	public static Long getLastCounter(Configuration conf) throws IOException{
		HTable h = new HTable(conf, COUNTER_TABLE);
		
		Get g = new Get(LAST_COUNTER_ROWKEY);
		g.addColumn(COLUMN_FAMILY, META_COL);
		Result r = h.get(g);
		byte []lastCounterBytes = r.getValue(COLUMN_FAMILY, META_COL);
		h.close();
		
		if (lastCounterBytes == null){
			System.err.println("Last counter not found in the META column");
			return null;
		}
		
		long lastCounter = Bytes.toLong(lastCounterBytes);
		if (lastCounter == COUNTER_LIMIT){
			System.err.println("The counter limit is reached: "+lastCounter);
			return null;
		}
		
		return lastCounter;
	}
	
	/**
	 * @param startKey
	 * @param endKey
	 * @param numRegions
	 * @return
	 */
	public static byte[][] getSplits(byte []startKey, byte []endKey, int numRegions) {
		
		if (numRegions <= 1)
			return new byte[0][];
		
		byte[][] splits = new byte[numRegions - 1][];
		BigInteger lowestKey = new BigInteger(1, startKey);
		BigInteger highestKey = new BigInteger(1, endKey);
		BigInteger range = highestKey.subtract(lowestKey);
		BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
		BigInteger key = lowestKey.add(regionIncrement);
		
		for (int i = 0; i < numRegions-1; i++) {
			byte []b = key.toByteArray();
			if (b.length > KEY_LENGTH)
				splits[i] = Bytes.tail(b, KEY_LENGTH);
			else
				splits[i] = Bytes.padHead(b, KEY_LENGTH-b.length);
			key = key.add(regionIncrement);
		}
		return splits;
	}
	
	/**
	 * 
	 * @param start
	 * @param end
	 * @param portion
	 */
	public static BigInteger getAddress(BigInteger range, BigInteger startAddress, long chunkCount, long totalCount ){
		BigInteger chunkSize = range.multiply(BigInteger.valueOf(chunkCount)).divide(BigInteger.valueOf(totalCount));
		
		return startAddress.add(chunkSize);
	}
	
	public static byte [] convertBigIntegerToAddress(BigInteger address, int length){
		byte []ret = address.toByteArray();
		int offset = ret.length-length;
		
		if (offset < 0){
			return Bytes.padHead(ret, Math.abs(offset));
		}
		else if (offset > 0){
			return Bytes.tail(ret, length);
		}
		else
			return ret;
	}
	
	
	/**
	 * @param numRegions
	 * @return
	 */
	public static byte [][] getString2IdSplits(int numRegions){
		
		//divide all elements to the desired number of regions, so that a proper load balance is maintained
		if (totalStringCount < numRegions){
			return null;
		}
		
		long elementsPerRegion = (totalStringCount+numRegions)/numRegions;
		byte [][]byteSplits = new byte[numRegions-1][];
		long counterPerRegion = 0;
		int splitIndex = 0;
		
		for (Map.Entry<Short, Long> entry : prefixCounters.entrySet()) {
			System.out.println(entry.getKey());
			
			if (counterPerRegion + entry.getValue() < elementsPerRegion){
				counterPerRegion += entry.getValue();
			}
			else{
				long remaining = elementsPerRegion - counterPerRegion;
				
				//build the range of the key without the prefix bytes
				int remainingLength = KEY_LENGTH-1;
				byte []remainingRange = new byte[remainingLength];
				Arrays.fill(remainingRange, (byte)0xff);
				BigInteger range = new BigInteger(1, remainingRange);
				byte []currentAddressBytes = new byte[remainingLength];
				BigInteger currentAddress = new BigInteger(1, currentAddressBytes);
					
				long currentPrefixCount = 0;
				if (remaining < elementsPerRegion){	
					byteSplits[splitIndex] = new byte[KEY_LENGTH];
					currentPrefixCount += remaining;
					
					currentAddress = getAddress(range, currentAddress, remaining, entry.getValue());
					byte []addressBytes = convertBigIntegerToAddress(currentAddress, remainingLength);
					
					//Bytes.putBytes(byteSplits[splitIndex], 0, completePrefix.getBytes(), 0, completePrefix.length());
					byteSplits[splitIndex][0] = entry.getKey().byteValue();
					Bytes.putBytes(byteSplits[splitIndex], 1, addressBytes, 0, remainingLength);
					splitIndex++;
				}
				
				//divide the remaining splits with this prefix
				long prefixRemaining = entry.getValue()-currentPrefixCount;
				
				while (prefixRemaining > elementsPerRegion){
					byteSplits[splitIndex] = new byte[KEY_LENGTH];
					//currentPrefixCount += elementsPerRegion;
					
					currentAddress = getAddress(range, currentAddress, elementsPerRegion, entry.getValue());
					byte []addressBytes = convertBigIntegerToAddress(currentAddress, remainingLength);
					
					System.out.println(splitIndex);
					//Bytes.putBytes(byteSplits[splitIndex], 0, completePrefix.getBytes(), 0, completePrefix.length());
					byteSplits[splitIndex][0] = entry.getKey().byteValue();
					Bytes.putBytes(byteSplits[splitIndex], 1, addressBytes, 0, remainingLength);
					splitIndex++;
					prefixRemaining -= elementsPerRegion;
				}
				
				counterPerRegion = prefixRemaining;
			}
		}
		
		byte [][] ret;
		if (splitIndex < numRegions-1){
			ret = new byte[splitIndex][];
			for (int i = 0; i < splitIndex; i++) {
				ret[i] = byteSplits[i];
			}
		}
		else
			ret = byteSplits;
		
		return ret;
	}
	
	/**
	 * @param numRegions
	 * @param startOffset - 0 or 1
	 * @return
	 */
	public static byte [][]getNonNumericalSplits(int numRegions, int startOffset){
		byte []startKey = new byte[KEY_LENGTH];
		byte []endKey = new byte[KEY_LENGTH];
		Arrays.fill(endKey, startOffset+Long.SIZE/8, KEY_LENGTH, (byte)0xff);
		/*for (int i = 0; i < endKey.length; i++) {
			endKey[i] = (byte)0xff;
		}*/
		
		long startKeyPrefix = startPartition << 24;
		Bytes.putLong(startKey, startOffset, startKeyPrefix);
		long endKeyPrefix = ((startPartition+numInputPartitions-1) << 24) | 0xffffffL;;
		Bytes.putLong(endKey, startOffset, endKeyPrefix);
		
		System.out.println("Start: "+String.format("%x", startKeyPrefix)+"; End: "+String.format("%x", endKeyPrefix));
		
		return getSplits(startKey, endKey, numRegions);
	}
	
	/**
	 * @param numRegions
	 * @return
	 */
	public static byte [][]getNumericalSplits(int numRegions){
		byte []startKey = new byte[KEY_LENGTH];
		startKey[0] = (byte)0x80;
		
		byte []endKey = new byte[KEY_LENGTH];
		Arrays.fill(endKey, (byte)0xff);
		
		return getSplits(startKey, endKey, numRegions);
	}
	
	/**
	 * @param numRegions
	 * @return
	 */
	public static byte [][]getObjectPrefixSplits(int numRegions){
		int numericalRegions = (int)Math.round((double)numRegions*((double)numericalCount/(double)(totalStringCount+numericalCount)));
		
		byte [][]nonNumSplits = getNonNumericalSplits(numRegions-numericalRegions, 1);
		byte [][]numSplits = getNumericalSplits(numericalRegions);
		
		byte [][]splits = new byte[numRegions-1][];
		for (int i = 0; i < nonNumSplits.length; i++) {
			splits[i] = nonNumSplits[i];
		}
		
		if (nonNumSplits.length < numRegions-1){
			splits[nonNumSplits.length] = new byte[KEY_LENGTH];
			splits[nonNumSplits.length][0] = (byte)0x80;
			for (int i = 0; i < numSplits.length; i++) {
				splits[i+nonNumSplits.length+1] = numSplits[i];
			}
		}
		return splits;
	}
	
	private void printSplits(byte [][]splits){
		System.out.println("Number of splits: "+splits.length);
		for (int i = 0; i < splits.length; i++) {
			for (int j = 0; j < splits[i].length; j++) {
				System.out.print(String.format("%02x ", splits[i][j]));
			}
			System.out.println();
		}
	}

	@Override
	public void create() throws Exception {
		HBaseAdmin admin = con.getAdmin();
		/*byte []startKey = new byte[33];
		byte []endKey = new byte[33];
		for (int i = 1; i < endKey.length; i++) {
			endKey[i] = (byte)0xff;
		}
		startKey[0]=0x20;
		endKey[0]=0x7F;
		*/
		//distribute the regions over the entire ID space for String2ID
		byte [][]splits = getString2IdSplits(NUM_REGIONS);
		System.out.println(" String2Id splits: ");
		printSplits(splits);
		
		createTable(admin, STRING2ID, splits, false);
		
		splits = getNonNumericalSplits(NUM_REGIONS, 0);
		
		System.out.println("Id2String splits: ");
		printSplits(splits);
		
		//createTable(admin, ID2STRING_TEST, splits, false);//TODO change back
		createTable(admin, ID2STRING, splits, true);
		
		createTable(admin, TABLE_NAMES[SPOC], splits, false);
		
		createTable(admin, TABLE_NAMES[POCS], splits, false);
		createTable(admin, TABLE_NAMES[CSPO], splits, false);
		createTable(admin, TABLE_NAMES[CPSO], splits, false);	
		
		
		splits = getObjectPrefixSplits(NUM_REGIONS);
		printSplits(splits);
		
		createTable(admin, TABLE_NAMES[OCSP], splits, false);
		createTable(admin, TABLE_NAMES[OSPC], splits, false);
	}

}
