package nl.vu.datalayer.hbase.schema;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Map;
import java.util.SortedMap;

import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.connection.NativeJavaConnection;

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
	public static final String SUFFIX_PROPERTY = "suffix_property";

	public static final byte [] COLUMN_FAMILY = "F".getBytes();
	public static final byte [] COLUMN_NAME = "".getBytes();
	
	public static final String []TABLE_NAMES = {"SPOC", "POCS", "OCSP", "CSPO", "CPSO", "OSPC"};
	
	//3 BaseIds + 1 TypedId (in the Object position of each table)
	public static final int KEY_LENGTH = 33;
	
	//mapping between the SPOC order and the order in other tables
	public static final int [][]ORDER = {{0,1,2,3}, {3,0,1,2}, {2,3,0,1}, {1,2,3,0}, {2,1,3,0}, {1,2,0,3}};
	
	//mapping between other tables and SPOC
	public static final int [][]TO_SPOC_ORDER = {{0,1,2,3}, {1,2,3,0}, {2,3,0,1}, {3,0,1,2}, {3,1,0,2}, {2,0,1,3}};
	public static final int [][]OFFSETS = {{0,8,16,25}, {25,0,8,17}, {17,25,0,9}, {8,16,24,0}, {16,8,24,0}, {9,17,0,25}};
	
	public static final int SPOC = 0;
	public static final int POCS = 1;
	public static final int OCSP = 2;
	public static final int CSPO = 3;
	public static final int CPSO = 4;
	public static final int OSPC = 5;
	
	public static final String STRING2ID = "String2Id";
	public static final String ID2STRING = "Id2String";
	
	//information for the table of Counters
	public static final String COUNTER_TABLE = "Counters";
	public static final byte []META_COL = "Meta".getBytes();
	public static final byte [] FIRST_COUNTER_ROWKEY = "FirstCounter".getBytes();
	public static final byte [] LAST_COUNTER_ROWKEY = "LastCounter".getBytes();
	public static final long COUNTER_LIMIT = 0x000000ffffffffffL;
	
	//public static final String STRING2ID_TEST = "String2IdTest";
	//public static final String ID2STRING_TEST = "Id2StringTest";
	
	//default number of initial regions
	public static final int NUM_REGIONS = 64;
	
	private int numInputPartitions;
	private long startPartition;
	
	private boolean onlyTriples = false;
	
	/**
	 * Histogram that stores the frequency of the last byte in each quad element 
	 * used for proper splits for the table String2Id
	 */
	//private SortedMap<Short, Long> prefixCounters = null;
	
	/**
	 * Number of non-numerical elements: includes URIs, bNodes and non-numerical Literals
	 */
	private long totalStringCount = 0;
	
	/**
	 * Number of numericals 
	 */
	private static long numericalCount = 0;
	
	private String schemaSuffix;
	
	public HBPrefixMatchSchema(HBaseConnection con, String schemaSuffix) {
		super();
		this.con = con;
		this.schemaSuffix = schemaSuffix;
	}
	
	/**
	 * Sets all the information required to do proper pre-splits when creating the tables making up this schema
	 * @param totalNumberOfStrings - number of non-numerical elements: including URIs, bNodes and non-numerical Literals
	 * @param numericalParam - number of numerical literals
	 * @param numPartitions  - number of partitions created in the Triple2Resource pass
	 * @param startPartitionParam - the partition from which Id generation started in the current bulk load 
	 * @param prefixCounter   - histogram containing character frequency for elements that will be stored in String2Id
	 */
	public void setTableSplitInfo(long totalNumberOfStrings, long numericalParam,
									int numPartitions, long startPartitionParam, 
									boolean onlyTriplesParam){
		startPartition = startPartitionParam;
		numInputPartitions = numPartitions;
		totalStringCount = totalNumberOfStrings;
		//prefixCounters = prefixCounter;
		numericalCount = numericalParam;
		onlyTriples = onlyTriplesParam; 
	}
	
	/*public static void setId2StringTableSplitInfo(int numPartitions, long startPartitionParam){
		startPartition = startPartitionParam;
		numInputPartitions = numPartitions;
	}
	
	public static void setString2IdTableSplitInfo(long totalNumberOfString, SortedMap<Short, Long> prefixCounter){
		totalStringCount = totalNumberOfString;
		prefixCounters = prefixCounter;
	}
	
	public static void setObjectPrefixTableSplitInfo(long totalNumberOfString, long numericalParam){
		numericalCount = numericalParam;
		totalStringCount = totalNumberOfString;
	}*/
	
	public void createCounterTable(HBaseAdmin admin) throws IOException{
		
		String fullName = COUNTER_TABLE+schemaSuffix;
		if (admin.tableExists(fullName) == false){
			HTableDescriptor desc = new HTableDescriptor(fullName);

			HColumnDescriptor famDesc = new HColumnDescriptor(COLUMN_FAMILY);
			famDesc.setMaxVersions(1);
			desc.addFamily(famDesc);

			System.out.println("Creating table: " + fullName);
			admin.createTable(desc);
			
			//initialize the values for the first and last counter names
			Put p = new Put(FIRST_COUNTER_ROWKEY);
			p.add(COLUMN_FAMILY, META_COL, Bytes.toBytes(0L));
			
			HTable h = new HTable(fullName);
			h.put(p);
			
			p = new Put(LAST_COUNTER_ROWKEY);
			p.add(COLUMN_FAMILY, META_COL, Bytes.toBytes(-1L));
			h.put(p);
			h.close();
		}
	}
	
	public static void updateCounter(int partitionId, long localRowCount, String schemaSuffix) throws IOException{
		Configuration conf = HBaseConfiguration.create();		
		long lastCounter = getLastCounter(conf, schemaSuffix);
		long newCounterColumn = lastCounter+partitionId+1;
		
		HTable h = new HTable(conf, COUNTER_TABLE+schemaSuffix);
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
	public static Long updateLastCounter(int numCounters, Configuration conf, String schemaSuffix) throws IOException{
		long val = getLastCounter(conf, schemaSuffix);
		
		if (COUNTER_LIMIT - val < numCounters){
			System.err.println(numCounters+" + "+val+" exceeds counter limit:");
			return null;
		}
		
		HTable h = new HTable(conf, COUNTER_TABLE+schemaSuffix);
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
	public static Long getLastCounter(Configuration conf, String schemaSuffix) throws IOException{
		HTable h = new HTable(conf, COUNTER_TABLE+schemaSuffix);
		
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
	
	@Override
	public void create() throws Exception {
		HBaseAdmin admin = ((NativeJavaConnection)con).getAdmin();
	
		//distribute the regions over the entire ID space for String2ID
		byte[][] splits = getString2IdSplits();
		
		createTable(admin, STRING2ID+schemaSuffix, splits, false);
		
		splits = getNonNumericalSplits(NUM_REGIONS, 0);
		
		System.out.println("Id2String splits: ");
		printSplits(splits);
		
		createTable(admin, ID2STRING+schemaSuffix, splits, true);
		
		createTable(admin, TABLE_NAMES[SPOC]+schemaSuffix, splits, false);
		
		createTable(admin, TABLE_NAMES[POCS]+schemaSuffix, splits, false);
		
		byte [][] objectSplits = getObjectPrefixSplits(NUM_REGIONS);
		printSplits(objectSplits);
		createTable(admin, TABLE_NAMES[OSPC]+schemaSuffix, objectSplits, false);
		
		if (onlyTriples == false){
			createTable(admin, TABLE_NAMES[CSPO]+schemaSuffix, splits, false);
			createTable(admin, TABLE_NAMES[CPSO]+schemaSuffix, splits, false);	
			
			createTable(admin, TABLE_NAMES[OCSP]+schemaSuffix, objectSplits, false);
		}
	}
	
	/**
	 * Creates an HBase table with specified splits and optional compression for the values in the table
	 * 
	 * @param admin
	 * @param tableName
	 * @param splits
	 * @param enableCompression
	 * @throws IOException
	 */
	public static void createTable(HBaseAdmin admin, String tableName, byte [][]splits, boolean enableCompression) throws IOException{
		if (admin.tableExists(tableName) == false){
			HTableDescriptor desc = new HTableDescriptor(tableName);
			HColumnDescriptor famDesc = new HColumnDescriptor(COLUMN_FAMILY);
			
			famDesc.setBloomFilterType(StoreFile.BloomType.ROW);
			famDesc.setMaxVersions(1);
			if (enableCompression){//by default it is disabled
				famDesc.setCompactionCompressionType(Algorithm.LZO);
				famDesc.setCompressionType(Algorithm.LZO);
			}
			desc.addFamily(famDesc);
			desc.setMaxFileSize(1024*1024*1024);//set maximum StoreFile size to a high value-1GB
												//since we're dealing with a lot of data

			System.out.println("Creating table: " + tableName);
			admin.createTable(desc, splits);
		}
	}
	
	/**
	 * Divide the space between startKey and endKey into an equal amount of regions
	 * 
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
	 * Splits the space between startPartition and lastPartition into equal parts
	 * 
	 * @param numRegions
	 * @param startOffset - 0 or 1
	 * @return
	 */
	final public byte [][]getNonNumericalSplits(int numRegions, int startOffset){
		byte []startKey = new byte[KEY_LENGTH];
		byte []endKey = new byte[KEY_LENGTH];
		Arrays.fill(endKey, startOffset+Long.SIZE/8, KEY_LENGTH, (byte)0xff);
		
		long startKeyPrefix = startPartition << 24;
		Bytes.putLong(startKey, startOffset, startKeyPrefix);
		long endKeyPrefix = ((startPartition+numInputPartitions-1) << 24) | 0xffffffL;;
		Bytes.putLong(endKey, startOffset, endKeyPrefix);
		
		System.out.println("Start: "+String.format("%x", startKeyPrefix)+"; End: "+String.format("%x", endKeyPrefix));
		
		return getSplits(startKey, endKey, numRegions);
	}
	
	/**
	 * Splits the space between 0x80 and 0xff in equal parts
	 * 
	 * @param numRegions
	 * @return
	 */
	final public byte [][]getNumericalSplits(int numRegions){
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
	final private byte [][]getObjectPrefixSplits(int numRegions){
		int numericalRegions = (int)Math.round((double)numRegions*((double)numericalCount/(double)(totalStringCount+numericalCount)));
		
		byte [][]nonNumSplits = getNonNumericalSplits(numRegions-numericalRegions, 1);
		byte [][]numSplits = getNumericalSplits(numericalRegions);
		
		byte [][]splits = new byte[numRegions-1][];
		for (int i = 0; i < nonNumSplits.length; i++) {
			splits[i] = nonNumSplits[i];
		}
		
		if (nonNumSplits.length < numRegions-1){
			splits[nonNumSplits.length] = new byte[KEY_LENGTH];
			splits[nonNumSplits.length][0] = (byte)0x80;//place the key between the numerical and non-numerical splits
			for (int i = 0; i < numSplits.length; i++) {
				splits[i+nonNumSplits.length+1] = numSplits[i];
			}
		}
		return splits;
	}
	
	final private void printSplits(byte [][]splits){
		System.out.println("Number of splits: "+splits.length);
		for (int i = 0; i < splits.length; i++) {
			for (int j = 0; j < splits[i].length; j++) {
				System.out.print(String.format("%02x ", splits[i][j]));
			}
			System.out.println();
		}
	}

	final private byte[][] getString2IdSplits() {
		byte []startKey = new byte[KEY_LENGTH];
		byte []endKey = new byte[KEY_LENGTH];
		Arrays.fill(endKey, 0, KEY_LENGTH, (byte)0xff);
		byte [][]splits = getSplits(startKey, endKey, NUM_REGIONS);
		System.out.println(" String2Id splits: ");
		printSplits(splits);
		return splits;
	}

	
	/**
	 * 
	 * @param start
	 * @param end
	 * @param portion
	 */
	/*public static BigInteger getAddress(BigInteger range, BigInteger startAddress, long chunkCount, long totalCount ){
		BigInteger chunkSize = range.multiply(BigInteger.valueOf(chunkCount)).divide(BigInteger.valueOf(totalCount));
		
		return startAddress.add(chunkSize);
	}
	
	/*public static byte [] convertBigIntegerToAddress(BigInteger address, int length){
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
	}*/
	
	
	/**
	 * Computes the splits based on the histogram stored in prefixCounters 
	 * 
	 * @param numRegions
	 * @return
	 */
	/*public byte [][] getString2IdSplits(int numRegions){
		
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
					
					byteSplits[splitIndex][0] = entry.getKey().byteValue();
					Bytes.putBytes(byteSplits[splitIndex], 1, addressBytes, 0, remainingLength);
					splitIndex++;
				}
				
				//divide the remaining splits with this prefix
				long prefixRemaining = entry.getValue()-currentPrefixCount;
				
				while (prefixRemaining > elementsPerRegion){
					byteSplits[splitIndex] = new byte[KEY_LENGTH];
					
					currentAddress = getAddress(range, currentAddress, elementsPerRegion, entry.getValue());
					byte []addressBytes = convertBigIntegerToAddress(currentAddress, remainingLength);
					
					System.out.println(splitIndex);
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
	}*/
	
}
