package nl.vu.datalayer.hbase.operations.prefixmatch;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Stack;

import nl.vu.datalayer.hbase.connection.AsyncNativeJavaConnection;
import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.connection.NativeJavaConnection;
import nl.vu.datalayer.hbase.exceptions.ElementNotFoundException;
import nl.vu.datalayer.hbase.exceptions.NonNumericalException;
import nl.vu.datalayer.hbase.exceptions.NumericalRangeException;
import nl.vu.datalayer.hbase.id.BaseId;
import nl.vu.datalayer.hbase.id.HBaseValue;
import nl.vu.datalayer.hbase.id.Id;
import nl.vu.datalayer.hbase.id.TypedId;
import nl.vu.datalayer.hbase.loader.HBaseLoader;
import nl.vu.datalayer.hbase.parameters.HBaseTripleElement;
import nl.vu.datalayer.hbase.parameters.Quad;
import nl.vu.datalayer.hbase.parameters.ResultRow;
import nl.vu.datalayer.hbase.parameters.RowLimitPair;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.hbase.async.HBaseClient;
import org.hbase.async.ScanFilter;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.sematext.hbase.wd.AbstractRowKeyDistributor;
import com.sematext.hbase.wd.DistributedScanner;
import com.sematext.hbase.wd.RowKeyDistributorByOneBytePrefix;

/**
 * Class that exposes operations with the tables in the PrefixMatch schema
 *
 */
public class HBPrefixMatchOperationManager implements IHBasePrefixMatchRetrieveOpsManager {
	
	private static final int OBJECT_POSITION = 2;

	private static final int QUAD_SIZE = 4;
	
	private HBaseConnection con;
	
	/**
	 * Maps the query patterns to the associated tables that resolve those patterns 
	 */
	private HashMap<String, PatternInfo> patternInfo;
	
	/**
	 * Internal use: the index of the table used for retrieval
	 */
	private int currentTableIndex;
	
	/**
	 * Variable storing time for the overhead of id2StringMap mappings upon retrieval
	 */
	private long id2StringOverhead = 0;
	
	/**
	 * Variable storing time for the overhead of String2Id mappings 
	 */
	private long string2IdOverhead = 0;
	
	private ArrayList<ArrayList<Id>> quadResults;
	
	private ArrayList<Id> boundElements;
	
	//private ValueFactory valueFactory;
	
	private String schemaSuffix;
	private int slaveNodeCount;
	
	private MessageDigest mDigest;

	private String currentPattern;

	private HashMap<ByteArray, Value> hash2ValueMap = new HashMap<ByteArray, Value>();

	private ArrayList<Get> batchGets;
	
	private HashMap<ByteArray, ArrayList<Id>> tempCache;
	
	private class PatternInfo{
		int tableIndex;
		int scannerCachingSize;
		int prefixSize;
		
		public PatternInfo(int tableIndex, int scannerCachingSize, int prefixSize) {
			super();
			this.tableIndex = tableIndex;
			this.scannerCachingSize = scannerCachingSize;
			this.prefixSize = prefixSize; 
		}
	}

	public HBPrefixMatchOperationManager(HBaseConnection con) {
		super();
		this.con = con;
		patternInfo = new HashMap<String, PatternInfo>(16);
		buildPattern2TableHashMap();
		quadResults = new ArrayList<ArrayList<Id>>();
		boundElements = new ArrayList<Id>();
		batchGets = new ArrayList<Get>();
		
		Properties prop = new Properties();
		try{
			prop.load(new FileInputStream("config.properties"));
			schemaSuffix = prop.getProperty(HBPrefixMatchSchema.SUFFIX_PROPERTY, "");
			slaveNodeCount = Integer.parseInt(prop.getProperty(HBPrefixMatchSchema.NUMBER_OF_SLAVE_NODES_PROPERTY, ""));
			
		}
		catch (IOException e) {
			//continue to use the default properties
		}
		
		try {
			mDigest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	
	private void buildPattern2TableHashMap(){
		patternInfo.put("????", new PatternInfo(HBPrefixMatchSchema.SPOC, 1000, 0));
		patternInfo.put("|???", new PatternInfo(HBPrefixMatchSchema.SPOC, 200, BaseId.SIZE));
		patternInfo.put("||??", new PatternInfo(HBPrefixMatchSchema.SPOC, 100, 2*BaseId.SIZE));
		patternInfo.put("|||?", new PatternInfo(HBPrefixMatchSchema.SPOC, 10, 2*BaseId.SIZE+TypedId.SIZE));
		patternInfo.put("||||", new PatternInfo(HBPrefixMatchSchema.SPOC, 1, 3*BaseId.SIZE+TypedId.SIZE));
		
		patternInfo.put("?|??", new PatternInfo(HBPrefixMatchSchema.POCS, 1000, BaseId.SIZE));
		patternInfo.put("?||?", new PatternInfo(HBPrefixMatchSchema.POCS, 500, BaseId.SIZE+TypedId.SIZE));
		patternInfo.put("?|||", new PatternInfo(HBPrefixMatchSchema.POCS, 100, 2*BaseId.SIZE+TypedId.SIZE));
		
		patternInfo.put("??|?", new PatternInfo(HBPrefixMatchSchema.OSPC, 300, TypedId.SIZE));//should be smaller if it's a literal
		patternInfo.put("|?|?", new PatternInfo(HBPrefixMatchSchema.OSPC, 50, BaseId.SIZE+TypedId.SIZE));
		
		patternInfo.put("??||", new PatternInfo(HBPrefixMatchSchema.OCSP, 100, BaseId.SIZE+TypedId.SIZE));
		patternInfo.put("|?||", new PatternInfo(HBPrefixMatchSchema.OCSP, 5, 2*BaseId.SIZE+TypedId.SIZE));
		
		patternInfo.put("???|", new PatternInfo(HBPrefixMatchSchema.CSPO, 400, BaseId.SIZE));
		patternInfo.put("|??|", new PatternInfo(HBPrefixMatchSchema.CSPO, 50, 2*BaseId.SIZE));
		patternInfo.put("||?|", new PatternInfo(HBPrefixMatchSchema.CSPO, 5, 3*BaseId.SIZE));
		
		patternInfo.put("?|?|", new PatternInfo(HBPrefixMatchSchema.CPSO, 200, 2*BaseId.SIZE));
	}

	//====================== RETRIEVAL FUNCTIONS =====================
	
	@Override
	public ArrayList<ResultRow> joinTriplePatterns(ArrayList<Quad> patterns, List<ByteBuffer> varEncodings,
														LinkedHashSet<String> qualifierNames) throws IOException{
		
		if (patterns.size() != varEncodings.size()){
			throw new IOException("You did not comply with \"THE JOIN PROTOCOL\" !!!");
		}
		
		if (patterns.size() == 0){
			return new ArrayList<ResultRow>();
		}
		
		AsyncScannerPool scannerPool = new AsyncScannerPool();
		byte []qualifier=null;
		
		ArrayList<byte []> joinTableQualifiers = new ArrayList<byte []>(varEncodings.size());
				
		//issue all triple patterns in parallel
		for (int i = 0; i < patterns.size(); i++) {
			try {		
				qualifier = new byte[varEncodings.get(i).remaining()];				
				varEncodings.get(i).get(qualifier);				
				extractJoinTableQualifier(qualifier, joinTableQualifiers);

				Quad quadPattern = patterns.get(i);
				byte[] rangeScanKey = buildRangeScanKeyFromQuad(quadPattern.getElems(), null);
				
				AsyncScanner scanner = buildAsyncScanner(qualifier, rangeScanKey);
				scannerPool.addNewScanner(scanner);
				
			} catch (NumericalRangeException e) {
				throw new IOException("Bound numerical variable not in expected range: " + e.getMessage());
			} catch (ElementNotFoundException e) {
				throw new IOException(e.getMessage());
			}
		}	
		short joinId = Bytes.toShort(qualifier);
			
		try {
			scannerPool.doParallelScan();//blocks until the intermediate table is populated by the coprocessors
		} catch (InterruptedException e) {
			throw new IOException("Synchronization problem while doing parallel scan: "+e.getMessage());
		}
		
		ResultScanner rs = buildJoinTableScanner(joinTableQualifiers, joinId);
		
		ArrayList<ResultRow> results = new ArrayList<ResultRow>();
		Result currentResult=null;	
	    while ((currentResult=rs.next())!=null){
	    	//int resultsSizeBefore = results.size();
	    	
	    	byte[] joinKey = currentResult.getRow();
	    	ArrayList<Id> joinIds = parseByteArrayIntoIds(joinKey, 3);//jump bucket id(1byte) and join id(2bytes)
	    	ResultRow start = new ResultRow(joinIds.size()+joinTableQualifiers.size());
	    	start.addAll(joinIds);
	    	tempCache = new HashMap<ByteArray, ArrayList<Id>>();
	    	buildResultsRecursively(results, joinTableQualifiers, 0, currentResult, start);
	    	
	    	/*TODO ASSUMING for now we don't have duplicates in the index tables
	    	int resultsSizeAfter = results.size();
	    	
	    	byte []multipBytes = currentResult.getValue(HBPrefixMatchSchema.JOIN_COL_FAMILY, 
	    											HBPrefixMatchSchema.JOIN_MULTIPLIER_QUAL_BYTES);
			if (multipBytes != null) {
				int multip = (int) (Bytes.toLong(multipBytes) - 1);
				for (int i = 0; i < multip*(resultsSizeAfter - resultsSizeBefore); i++) {
					ResultRow newResult = new ResultRow(results.get(i
							+ resultsSizeBefore));
					results.add(newResult);
				}
			}*/
	    }
		
		return results;
	}

	private AsyncScanner buildAsyncScanner(byte[] qualifier, byte[] rangeScanKey) {
		AsyncScanner scanner = new AsyncScanner(((AsyncNativeJavaConnection)con).getAsyncClient(),
				HBPrefixMatchSchema.TABLE_NAMES[currentTableIndex]+schemaSuffix,
				rangeScanKey, HBPrefixMatchSchema.COLUMN_FAMILY,
				qualifier,//encode (join id, triple id, join position, variable ids)
							//as col qualifier -> to be processed by the coprocessor
				patternInfo.get(currentPattern).scannerCachingSize);
		
		ScanFilter prefixFilter = new org.hbase.async.PrefixFilter(rangeScanKey);
		ScanFilter keyOnlyFilter = new org.hbase.async.FirstKeyOnlyFilter();
		ArrayList<ScanFilter> filters = new ArrayList<ScanFilter>();
		filters.add(prefixFilter);
		filters.add(keyOnlyFilter);
		ScanFilter filterList = new org.hbase.async.FilterList(filters);
		scanner.setFilter(filterList);
		return scanner;
	}

	private ResultScanner buildJoinTableScanner(ArrayList<byte[]> joinTableQualifiers, short joinId) throws IOException {
		HTableInterface joinTable = con.getTable(HBPrefixMatchSchema.JOIN_TABLE);
		
		byte []dummy=new byte[0];    
	    FilterList qualifierFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		for (byte []joinQualifier : joinTableQualifiers) {
			SingleColumnValueFilter colFilter = new SingleColumnValueFilter(HBPrefixMatchSchema.JOIN_COL_FAMILY, 
								joinQualifier, CompareOp.NOT_EQUAL, dummy);
			colFilter.setFilterIfMissing(true);
			qualifierFilters.addFilter(colFilter);
		}
		
		byte bucketsCount = slaveNodeCount > 256 ? (byte)256 : (byte)slaveNodeCount;
		AbstractRowKeyDistributor keyDistributor = new RowKeyDistributorByOneBytePrefix(bucketsCount);
		
		Scan scan = new Scan(Bytes.toBytes(joinId), Bytes.toBytes((short)((int)joinId+1)));
		scan.setFilter(qualifierFilters);
		
		
		ResultScanner rs = DistributedScanner.create(joinTable, scan, keyDistributor);
		return rs;
	}

	private void buildResultsFromValueStack(ArrayList<ResultRow> results, Stack<Pair<ArrayList<Id>, Iterator<Id>>> valueStack) {
		while (!valueStack.empty()){
			Pair<ArrayList<Id>, Iterator<Id>> top = valueStack.pop();
			Iterator<Id> it = top.getSecond();
				
			while (it.hasNext()){
				ResultRow resultRow = new ResultRow(top.getFirst());
				resultRow.add(it.next());
				results.add(resultRow);
			}
		}
	}

	private Stack<Pair<ArrayList<Id>, Iterator<Id>>> buildJoinRowStack(ArrayList<byte[]> joinTableQualifiers, Result current, ArrayList<Id> joinIds) {
		ArrayList<Id> parent = joinIds;
		Stack<Pair<ArrayList<Id>, Iterator<Id>>> valueStack = new Stack<Pair<ArrayList<Id>, Iterator<Id>>>();
		for (byte[] joinQualifier : joinTableQualifiers) {
			byte[] valuesArray = current.getValue(HBPrefixMatchSchema.JOIN_COL_FAMILY, joinQualifier);
			ArrayList<Id> ids = parseByteArrayIntoIds(valuesArray, 0);
			
			ArrayList<Id> start = new ArrayList<Id>(parent);
			if (ids.size()>0){
				Iterator<Id> it = ids.iterator();
				valueStack.add(new Pair<ArrayList<Id>, Iterator<Id>>(start, it));
				parent.add(ids.get(0));
			}
		}
		return valueStack;
	}
	
	private void buildResultsRecursively(ArrayList<ResultRow> results, 
							ArrayList<byte[]> joinTableQualifiers, int index, Result current, ResultRow currentRow) {
		
		byte[] valuesArray = current.getValue(HBPrefixMatchSchema.JOIN_COL_FAMILY, joinTableQualifiers.get(index));
		if (valuesArray.length==1 && valuesArray[0]==0x00){
			if (index<joinTableQualifiers.size()-1){
				buildResultsRecursively(results, joinTableQualifiers, index+1, current, currentRow);
			}
			else{
				results.add(currentRow);
			}
			return;
		}
		
		ArrayList<Id> columnIds = tempCache.get(new ByteArray(valuesArray));
		if (columnIds==null){
			columnIds = parseByteArrayIntoIds(valuesArray, 0);
			tempCache.put(new ByteArray(valuesArray), columnIds);
		}
		
		for (Id id : columnIds) {
			ResultRow newRow = new ResultRow(currentRow);
			newRow.add(id);
			
			if (index==joinTableQualifiers.size()-1){	
				results.add(newRow);
			}
			else{
				buildResultsRecursively(results, joinTableQualifiers, index+1, current, newRow);
			}
		}
	}

	private ArrayList<Id> parseByteArrayIntoIds(byte[] byteArray, int offset) {
		int currentOffset = offset;
		ArrayList<Id> ret = new ArrayList<Id>();
		
		while (currentOffset<byteArray.length){
			if (TypedId.getType(byteArray[currentOffset]) == TypedId.NUMERICAL){
				byte []idBytes = new byte[TypedId.SIZE];
				System.arraycopy(byteArray, currentOffset, idBytes, 0, TypedId.SIZE);
				Id newId = new TypedId(idBytes);
				ret.add(newId);
				currentOffset+=TypedId.SIZE;
			}
			else{//==TypedId.STRING
				byte []idBytes = new byte[BaseId.SIZE];
				System.arraycopy(byteArray, currentOffset, idBytes, 0, BaseId.SIZE);
				Id newId = new BaseId(idBytes);
				ret.add(newId);
				currentOffset+=BaseId.SIZE;
			}
		}
		
		return ret;
	}

	private void extractJoinTableQualifier(byte[] qualifier, ArrayList<byte[]> joinTableQualifiers) {
		byte []joinTableQualifier;
		if (qualifier.length>4){
			for (int j=4; j<qualifier.length; j++){
				joinTableQualifier = new byte[]{qualifier[2], qualifier[j]};
				joinTableQualifiers.add(joinTableQualifier);
			}
		}
		else if (qualifier.length==4){
			joinTableQualifier = new byte[]{qualifier[2]};
			joinTableQualifiers.add(joinTableQualifier);
		}
		else{
			throw new RuntimeException("Qualifier length < 3 in joinTriplePatterns call");
		}
	}
	
	@Override
	public ArrayList<ArrayList<Id>> getResults(Quad quad)
			throws IOException {		
		return getResults(quad, null);
	}
	
	@Override
	public ArrayList<ArrayList<Id>> getResults(Quad quad, RowLimitPair limits) throws IOException {
		try {
			quadResults.clear();
			boundElements.clear();
			byte[] keyPrefix = buildRangeScanKeyFromQuad(quad.getElems(), limits);
			
			//do the range scan
			if (limits!=null){
				doRangeScan(keyPrefix, limits);
			}
			else{
				doRangeScan(keyPrefix);
			}
			
			return buildSPOCOrderIdQuads();
		} catch (NumericalRangeException e) {
			throw new IOException( "Bound numerical variable not in expected range: " + e.getMessage());
		} catch (ElementNotFoundException e) {
			throw new IOException(e.getMessage());
		}
	}
	
	final private byte []buildRangeScanKeyFromQuad(Id []quad, RowLimitPair limitPair) throws IOException, NumericalRangeException, ElementNotFoundException
	{
		currentPattern = "";
		
		buildTriplePattern(quad, limitPair);
		currentTableIndex = patternInfo.get(currentPattern).tableIndex;
		int keySize = patternInfo.get(currentPattern).prefixSize;
		if (limitPair!=null){
			keySize -= TypedId.SIZE;
		}
		
		byte []key = new byte[keySize];
		//string2IdOverhead += System.currentTimeMillis()-start;
		key = buildRangeScanKeyFromMappedIds(quad, key);
		
		return key;
	}
	
	final private void buildTriplePattern(HBaseTripleElement[] quad, RowLimitPair limitPair) {
		for (int i = 0; i < quad.length; i++) {
			if ((quad[i] != null) || 
					(i==OBJECT_POSITION && limitPair!=null)) {
				currentPattern += "|";
			} 
			else {
				currentPattern += "?";
			}
		}
	}
	
	final private byte[] buildRangeScanKeyFromMappedIds(Id[] quad, byte []key) throws NumericalRangeException, ElementNotFoundException, IOException {
		for (int i = 0; i < quad.length; i++) {
			if (quad[i] != null) {
				boundElements.add(quad[i]);
				int offset = HBPrefixMatchSchema.OFFSETS[currentTableIndex][i];
				
				if (i != OBJECT_POSITION) {
					Bytes.putBytes(key, offset, quad[i].getBytes(), 0, BaseId.SIZE);
				} else {// OBJECT position
					if (quad[i] instanceof TypedId && ((TypedId) quad[i]).getType() == TypedId.NUMERICAL) {
						Bytes.putBytes(key, offset, quad[i].getBytes(), 0, TypedId.SIZE);
					} else {
						Bytes.putBytes(key, offset + 1, quad[i].getBytes(), 0, BaseId.SIZE);
					}
				}
			}
		}
		
		return key;
	}
	
	final private void doRangeScan(byte[] startKey) throws IOException {
		Filter prefixFilter = new PrefixFilter(startKey);
		Filter keyOnlyFilter = new FirstKeyOnlyFilter();
		Filter filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, prefixFilter, keyOnlyFilter);
		
		Scan scan = new Scan(startKey, filterList);
		scan.setCaching(patternInfo.get(currentPattern).scannerCachingSize);
		scan.setCacheBlocks(false);

		String tableName = HBPrefixMatchSchema.TABLE_NAMES[currentTableIndex]+schemaSuffix;
		
		//System.out.println("Retrieving from table: "+tableName);
		
		HTableInterface table = con.getTable(tableName);
		ResultScanner results = table.getScanner(scan);

		parseRangeScanResults(startKey.length, results);
		table.close();
	}

	final private ArrayList<ArrayList<Id>> parseRangeScanResults(int startKeyLength, ResultScanner results) throws IOException {
		Result r = null;
		int sizeOfInterest = HBPrefixMatchSchema.KEY_LENGTH - startKeyLength;
 
		try{
			while ((r = results.next()) != null) {
				ArrayList<Id> currentQuad = parseKey(r.getRow(), startKeyLength, sizeOfInterest);
				quadResults.add(currentQuad);	
			}
		}
		finally{
			results.close();
		}
		//System.out.println("Range scan returned: "+i+" results");
		return quadResults;
	}
		
	final private ArrayList<Id> parseKey(byte []key, int startIndex, int sizeOfInterest){
		
		int elemNo = sizeOfInterest/BaseId.SIZE;
		ArrayList<Id> currentQuad = new ArrayList<Id>(elemNo);
		
		int crtIndex = startIndex;
		for (int i = 0; i < elemNo; i++) {
			int length;
			byte [] elemKey;
			if (crtIndex == HBPrefixMatchSchema.OFFSETS[currentTableIndex][2]){//for the Object position
				length = TypedId.SIZE;
				if (TypedId.getType(key[crtIndex]) == TypedId.STRING){
					elemKey = new byte[BaseId.SIZE];
					System.arraycopy(key, crtIndex+1, elemKey, 0, BaseId.SIZE);	
				}
				else{//numericals
					elemKey = new byte[TypedId.SIZE];
					System.arraycopy(key, crtIndex, elemKey, 0, TypedId.SIZE);
					crtIndex += length;
					Id newElem = new TypedId(elemKey);
					currentQuad.add(newElem);
					continue;
				}
			}
			else{//for non-Object positions
				length = BaseId.SIZE;
				elemKey = new byte[length];
				System.arraycopy(key, crtIndex, elemKey, 0, length);
			}
			
			Id newElem = new BaseId(elemKey);
			currentQuad.add(newElem);
			
			crtIndex += length;
		}
		
		return currentQuad;
	}
		
	final private void doRangeScan(byte[] prefix, RowLimitPair limits) throws IOException {
		byte []startKey = Bytes.add(prefix, limits.getStartLimit().getBytes());
		byte []endKey = getAdjustedEndKey(Bytes.add(prefix, limits.getEndLimit().getBytes()), startKey);
		
		Filter keyOnlyFilter = new FirstKeyOnlyFilter();
		
		Scan scan = new Scan(startKey, endKey);
		scan.setFilter(keyOnlyFilter);
		
		scan.setCaching(patternInfo.get(currentPattern).scannerCachingSize);
		scan.setCacheBlocks(false);

		String tableName = HBPrefixMatchSchema.TABLE_NAMES[currentTableIndex]+schemaSuffix;
		
		//System.out.println("Retrieving from table: "+tableName);
		
		HTableInterface table = con.getTable(tableName);
		ResultScanner results = table.getScanner(scan);

		parseRangeScanResults(prefix.length, results);
		table.close();
	}

	private byte[] getAdjustedEndKey(byte []endKey, byte[] startKey) {
		byte[] adjustedEndKey;
		BigInteger temp = new BigInteger(1, endKey);
		temp = temp.add(BigInteger.valueOf(1));
		
		byte []b = temp.toByteArray();
		if (b.length > startKey.length){
			adjustedEndKey = Bytes.tail(b, startKey.length);
		}
		else{
			adjustedEndKey = Bytes.padHead(b, startKey.length-b.length);
		}
		return adjustedEndKey;
	}

	private ArrayList<ArrayList<Id>> buildSPOCOrderIdQuads() {
		int numberOfBoundElements = boundElements.size();
		ArrayList<ArrayList<Id>> ret = new ArrayList<ArrayList<Id>>(quadResults.size());
		Id[] initValue = new Id[]{null, null, null, null};
		ArrayList<Id> initList = new ArrayList<Id>();//using an ArrayList will prevent an extra copy
														//when creating newQuadResult during the loop
														//-> see constructor ArrayList(Collection<? extends E> c)
		initList.addAll(Arrays.asList(initValue));
		
		for (ArrayList<Id> quadList : quadResults) {
			ArrayList<Id> newQuadResult = new ArrayList<Id>(initList);			
			
			fillInUnboundPositionsWithIds(numberOfBoundElements, quadList, newQuadResult);		
			fillInBoundPositionsWithIds(newQuadResult);
			
			ret.add(newQuadResult);
		}
		
		return ret;
	}

	private void fillInBoundPositionsWithIds(ArrayList<Id> newQuadResult) {
		for (int i = 0, j = 0; i<newQuadResult.size() && j<boundElements.size(); i++) {
			if (newQuadResult.get(i) == null){
				Id boundElement = boundElements.get(j++);
				newQuadResult.set(i, boundElement);
			}
		}
	}

	private void fillInUnboundPositionsWithIds(int numberOfBoundElements, ArrayList<Id> quadList, ArrayList<Id> newQuadResult) {
		int offset = numberOfBoundElements;
		for (int i = 0; i < quadList.size(); i++) {
			Id currentElem = quadList.get(i);
			
			newQuadResult.set(HBPrefixMatchSchema.TO_SPOC_ORDER[currentTableIndex][offset], currentElem);
			offset++;
		}
	}

	@Override
	public ArrayList<ArrayList<Value>> getResults(Value[] quad)
			throws IOException {
		
		Map<Value, Id> value2IdMap = new HashMap<Value, Id>();
		for (Value value : quad) {
			value2IdMap.put(value, null);
		}
		
		mapValuesToIds(value2IdMap);
		
		Id []idBasedQuadElements = new Id[quad.length];
		for (int i = 0; i < idBasedQuadElements.length; i++) {
			idBasedQuadElements[i] = value2IdMap.get(quad[i]);
		}
		
		ArrayList<ArrayList<Id>> idResults = getResults(new Quad(idBasedQuadElements));
		
		Map<Id, Value> id2ValueMap = new HashMap<Id, Value>();
		for (ArrayList<Id> arrayList : idResults) {
			for (Id id : arrayList) {
				if (!id2ValueMap.containsKey(id)){
					id2ValueMap.put(id, null);
				}
			}
		}
		
		materializeIds(id2ValueMap);
		
		ArrayList<ArrayList<Value>> results = new ArrayList<ArrayList<Value>>(idResults.size());
		for (ArrayList<Id> idQuad : idResults) {
			ArrayList<Value> newQuad =  new ArrayList<Value>(4);
			for (Id id : idQuad) {
				newQuad.add(id2ValueMap.get(id));
			}
			results.add(newQuad);
		}
		return results;
	}
	
	//======================= LOADING FUNCTIONS ===============
	
	@Override
	public void populateTables(ArrayList<Statement> statements)
			throws Exception {
		HBaseLoader loader = new HBaseLoader(this, con, schemaSuffix);
		loader.load(statements);
	}	
	
	//========================== HELPER OR UNIMPLEMENTED ================================ 
	
	@Override
	public long countResults(Value[] quad, long hardLimit) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public static String hexaString(byte []b){
		String ret = "";
		for (int i = 0; i < b.length; i++) {
			ret += String.format("\\x%02x", b[i]);
		}
		return ret;
	}
	
	@Override
	public byte []retrieveId(Value val) throws IOException{
		byte []sBytes = val.toString().getBytes("UTF-8");
		byte []md5Hash = mDigest.digest(sBytes);
		
		Get g = new Get(md5Hash);
		g.addColumn(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
		
		HTableInterface table = con.getTable(HBPrefixMatchSchema.STRING2ID+schemaSuffix);
		Result r = table.get(g);
		byte []id = r.getValue(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
		//if (id == null){
			//System.err.println("Id does not exist for: "+s);
		//}
		
		return id;
	}
	
	//===================================== MAPPING FUNCTIONS ===========================================
	@Override
	public void materializeIds(Map<Id, Value> id2ValueMap) throws IOException {
		batchGets.clear();
		
		for (Map.Entry<Id, Value> mapEntry : id2ValueMap.entrySet()) {
			Id id = mapEntry.getKey();
			Get g;
			if (id instanceof BaseId){
				g = new Get(id.getBytes());
			}
			else if (id instanceof TypedId && ((TypedId)id).getType()==TypedId.STRING){
				g = new Get(id.getContent());
			}
			else{//numeric TypedId
				mapEntry.setValue(((TypedId)id).toLiteral());
				continue;
			}
					
			batchGets.add(g);
		}
		
		try{
			Result[] results = doBatchId2Value();
			updateId2ValueMap(results, id2ValueMap);
		}
		catch (ElementNotFoundException e) {
			throw new IOException(e.getMessage());
		} 	
	}
	
	final private Result[] doBatchId2Value() throws IOException {
		HTableInterface id2StringTable = con.getTable(HBPrefixMatchSchema.ID2STRING+schemaSuffix);
		Result []id2StringResults = id2StringTable.get(batchGets);
		id2StringTable.close();
		return id2StringResults;
	}
	
	final private void updateId2ValueMap(Result[] id2StringResults, Map<Id, Value> id2ValueMap) throws IOException, ElementNotFoundException {
		HBaseValue hbaseValue = new HBaseValue();
		byte []temp = {};
		//we create the byte stream in advance to avoid reallocation for each result
		ByteArrayInputStream byteStream = new ByteArrayInputStream(temp);
		DataInputStream dataInputStream = new DataInputStream(byteStream);
		
		for (Result result : id2StringResults) {
			byte []rowVal = result.getValue(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
			byte []rowKey = result.getRow();
			if (rowVal == null || rowKey == null){
				throw new ElementNotFoundException("Id not found in Id2String table: "+(rowKey == null ? null : hexaString(rowKey)));
			}
			else{
				byteStream.setArray(rowVal);		
				hbaseValue.readFields(dataInputStream);
				Value val = hbaseValue.getUnderlyingValue();
				id2ValueMap.put(new BaseId(rowKey), val);
			}
		}
	}

	@Override
	public void mapValuesToIds(Map<Value, Id> value2IdMap) throws IOException {
		hash2ValueMap.clear();
		batchGets.clear();
		
		ArrayList<Get> hbaseGetOps;
		try {
			hbaseGetOps = buildValue2IdGetsAndMapNumericals(value2IdMap);
			Result[] results = doValue2IdMapping(hbaseGetOps);
			
			for (Result result : results) {
				byte[] idBytes = result.getValue(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
				if (idBytes == null) {
					//TODO replace with error logging throw new IOException("Quad element not found: " + new String(result.toString()) + "\n" 
						//							+ (result.getRow() == null ? null : hexaString(result.getRow())));
					continue;
				}
				
				Value toUpdate = hash2ValueMap.get(new ByteArray(result.getRow()));
				value2IdMap.put(toUpdate, new BaseId(idBytes));
			}
		} catch (NumericalRangeException e) {
			throw new IOException(e.getMessage());
		}
		
	}
	
	private ArrayList<Get> buildValue2IdGetsAndMapNumericals(Map<Value, Id> value2IdMap) throws UnsupportedEncodingException, NumericalRangeException {
		ArrayList<Get> string2IdGets = new ArrayList<Get>();
		
		for (Map.Entry<Value, Id> mapEntry : value2IdMap.entrySet()) {
			Value val = mapEntry.getKey();

			byte[] sBytes;
			if (val instanceof Literal) {// literal
				Literal l = (Literal) val;
				if (l.getDatatype() != null) {
					try {
						TypedId id = TypedId.createNumerical(l);
						mapEntry.setValue(id);
						continue;
					} catch (NonNumericalException e) {
					}
				}
				String lString = l.toString();
				sBytes = lString.getBytes("UTF-8");
			} else {// bNode or URI
				sBytes = val.toString().getBytes("UTF-8");
			}

			byte[] md5Hash = mDigest.digest(sBytes);
			Get g = new Get(md5Hash);
			g.addColumn(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
			string2IdGets.add(g);
			hash2ValueMap.put(new ByteArray(md5Hash), val);
		}
		
		return string2IdGets;
	}
	
	private Result[] doValue2IdMapping(ArrayList<Get> string2IdGets)
			throws IOException {
		HTableInterface table = con.getTable(HBPrefixMatchSchema.STRING2ID+schemaSuffix);
		Result []results = table.get(string2IdGets);
		table.close();
		return results;
	}

}
