package nl.vu.datalayer.hbase.util;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

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
import nl.vu.datalayer.hbase.retrieve.HBaseGeneric;
import nl.vu.datalayer.hbase.retrieve.IHBasePrefixMatchRetrieve;
import nl.vu.datalayer.hbase.retrieve.IdWrapper;
import nl.vu.datalayer.hbase.retrieve.RowLimitPair;
import nl.vu.datalayer.hbase.retrieve.ValueWrapper;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Class that exposes operations with the tables in the PrefixMatch schema
 *
 */
public class HBPrefixMatchUtil implements IHBasePrefixMatchRetrieve {
	
	private static final int OBJECT_POSITION = 2;

	private static final int MAX_RESULTS = 1000000;
	
	public static final byte MAP_IDS_ON = 0;
	public static final byte MAP_IDS_OFF = 1;

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
	
	/**
	 * Internal map that stores associations between Ids in the results and the corresponding Value objects
	 */
	private HashMap<Id, Value> id2ValueMap;
	
	private ArrayList<ArrayList<Id>> quadResults;
	
	private ArrayList<HBaseGeneric> boundElements;
	
	private ValueFactory valueFactory;
	
	private String schemaSuffix;
	
	private MessageDigest mDigest;

	//private byte [] numericalElement;

	private String currentPattern;

	private HashMap<ByteArray, Integer> spocOffsetMap = new HashMap<ByteArray, Integer>();

	private ArrayList<Get> batchGets;
	
	private HBaseGeneric []genericPattern = {null, null, null, null};
	private ValueWrapper []valuePattern;

	public HBPrefixMatchUtil(HBaseConnection con) {
		super();
		this.con = con;
		patternInfo = new HashMap<String, PatternInfo>(16);
		buildPattern2TableHashMap();
		id2ValueMap = new HashMap<Id, Value>();
		quadResults = new ArrayList<ArrayList<Id>>();
		boundElements = new ArrayList<HBaseGeneric>();
		batchGets = new ArrayList<Get>();
		valueFactory = new ValueFactoryImpl();
		valuePattern = new ValueWrapper[4];
		for (int i = 0; i < valuePattern.length; i++) {
			valuePattern[i] = new ValueWrapper(null);
		}
		
		Properties prop = new Properties();
		try{
			prop.load(new FileInputStream("config.properties"));
			schemaSuffix = prop.getProperty(HBPrefixMatchSchema.SUFFIX_PROPERTY, "");	
			
			if (con instanceof NativeJavaConnection){
				initTablePool((NativeJavaConnection)con);
			}
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

	private void initTablePool(NativeJavaConnection con) throws IOException {
		String []tableNames = new String[HBPrefixMatchSchema.TABLE_NAMES.length+2];
		tableNames[0] = HBPrefixMatchSchema.STRING2ID + schemaSuffix;
		tableNames[1] = HBPrefixMatchSchema.ID2STRING + schemaSuffix;
		for (int i = 0; i < HBPrefixMatchSchema.TABLE_NAMES.length; i++) {
			tableNames[i+2] = HBPrefixMatchSchema.TABLE_NAMES[i]+schemaSuffix;
		}
		
		con.initTables(tableNames);
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

	public ArrayList<ArrayList<String>> getResults(String[] quad){
		return null;
	}
	
	
	@Override
	public ArrayList<ArrayList<Value>> getMaterializedResults(HBaseGeneric[] quad)
			throws IOException {//we expect the pattern in the order SPOC	
		return getMaterializedResults(quad, null);
	}
	
	@Override
	public ArrayList<ArrayList<Value>> getMaterializedResults(HBaseGeneric[] quad, RowLimitPair limits) throws IOException {
		try {
			retrievalInit();
			
			//long start = System.currentTimeMillis();
			//convertQuadToGenericPattern(quad);
			byte[] keyPrefix = buildRangeScanKeyFromQuad(quad, limits);
			//long keyBuildOverhead = System.currentTimeMillis()-start;

			//long startSearch = System.currentTimeMillis();
			if (limits!=null){
				doRangeScan(keyPrefix, limits, MAP_IDS_ON);
			}
			else{
				doRangeScan(keyPrefix, MAP_IDS_ON);
			}
			//long searchTime = System.currentTimeMillis() - startSearch;

			//long startId2String = System.currentTimeMillis();
			Result[] id2ValueResults = doBatchId2Value();
			//id2StringOverhead = System.currentTimeMillis()-startId2String;
			
			updateId2ValueMap(id2ValueResults);			
			
			ArrayList<ArrayList<Value>> ret = buildSPOCOrderValueResults();
			
			/*long totalTime = System.currentTimeMillis() - start;
			System.out.println("Id2String number of results: "+id2StringResults.length);
			System.out.println("TotalTime: "+totalTime+"; RangeScanOverhead: "+searchTime+"; Id2StringOverhead: "+id2StringOverhead+"; String2IdOverhead: "
									+string2IdOverhead+"; KeyBuildOverhead: "+keyBuildOverhead);
			*/
			return ret;
		
		} catch (NumericalRangeException e) {
			throw new IOException("Bound numerical variable not in expected range: "+e.getMessage());
		} catch (ElementNotFoundException e) {
			throw new IOException(e.getMessage());
		} 
	}

	final private ArrayList<ArrayList<Value>> buildSPOCOrderValueResults() {
		int numberOfBoundElements = boundElements.size();
		ArrayList<ArrayList<Value>> ret = new ArrayList<ArrayList<Value>>(quadResults.size());
		Value[] initValue = new Value[]{null, null, null, null};
		ArrayList<Value> initList = new ArrayList<Value>();//using an ArrayList will prevent an extra copy
														//when creating newQuadResult during the loop
														//-> see constructor ArrayList(Collection<? extends E> c)
		initList.addAll(Arrays.asList(initValue));
		
		for (ArrayList<Id> quadList : quadResults) {
			ArrayList<Value> newQuadResult = new ArrayList<Value>(initList);			
			
			fillInUnboundPositions(numberOfBoundElements, quadList, newQuadResult);		
			fillInBoundPositions(newQuadResult);
			
			ret.add(newQuadResult);
		}
		return ret;
	}

	final private void fillInBoundPositions(ArrayList<Value> newQuadResult) {
		for (int i = 0, j = 0; i<newQuadResult.size() && j<boundElements.size(); i++) {
			if (newQuadResult.get(i) == null){
				HBaseGeneric boundElement = boundElements.get(j++);
				if (boundElement instanceof ValueWrapper){
					newQuadResult.set(i, ((ValueWrapper)boundElement).getValue());
				}
				else{
					newQuadResult.set(i, id2ValueMap.get(((IdWrapper)boundElement).getId()));
				}
			}
		}
	}

	final private void fillInUnboundPositions(int numberOfBoundElements, ArrayList<Id> quadList, ArrayList<Value> newQuadResult) {
		for (int i = 0; i < quadList.size(); i++) {
			Id currentElem = quadList.get(i);
			if ((i+numberOfBoundElements) == HBPrefixMatchSchema.ORDER[currentTableIndex][OBJECT_POSITION] && 
					currentElem instanceof TypedId && ((TypedId)currentElem).getType() == TypedId.NUMERICAL){
				//handle numericals
				newQuadResult.set(OBJECT_POSITION, ((TypedId)currentElem).toLiteral());
			}
			else{
				newQuadResult.set(HBPrefixMatchSchema.TO_SPOC_ORDER[currentTableIndex][(i+numberOfBoundElements)], 
						id2ValueMap.get(currentElem));
			}
		}
	}

	final private void updateId2ValueMap(Result[] id2StringResults) throws IOException, ElementNotFoundException {
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

	final private Result[] doBatchId2Value() throws IOException {
		//TODO add gets for Ids in the bounded positions and add them to the map
		HTableInterface id2StringTable = con.getTable(HBPrefixMatchSchema.ID2STRING+schemaSuffix);
		Result []id2StringResults = id2StringTable.get(batchGets);
		id2StringTable.close();
		return id2StringResults;
	}

	final private void doRangeScan(byte[] startKey, byte mapIdsSwitch) throws IOException {
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

		parseRangeScanResults(startKey.length, results, mapIdsSwitch);
		table.close();
	}

	final private ArrayList<ArrayList<Id>> parseRangeScanResults(int startKeyLength, ResultScanner results, byte mapIdsSwitch) throws IOException {
		Result r = null;
		int sizeOfInterest = HBPrefixMatchSchema.KEY_LENGTH - startKeyLength;
 
		int i = 0;
		try{
			while ((r = results.next()) != null && i<MAX_RESULTS) {
				ArrayList<Id> currentQuad = parseKey(r.getRow(), startKeyLength, sizeOfInterest);
				quadResults.add(currentQuad);
				
				if (mapIdsSwitch == MAP_IDS_ON){
					buildId2ValueInfo(currentQuad);
				}
				i++;
			}
		}
		finally{
			results.close();
		}
		//System.out.println("Range scan returned: "+i+" results");
		return quadResults;
	}

	private void buildId2ValueInfo(ArrayList<Id> currentQuad) {
		for (Id id : currentQuad) {
			if (id instanceof BaseId && id2ValueMap.get(id) == null){
				id2ValueMap.put(id, null);
				Get g = new Get(((BaseId)id).getBytes());
				g.addColumn(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
				batchGets.add(g);
			}
		}
	}

	final private void retrievalInit() {
		id2ValueMap.clear();
		quadResults.clear();
		batchGets.clear();
		boundElements.clear();
		id2StringOverhead = 0;
		string2IdOverhead = 0;
	}
	
	public static String hexaString(byte []b){
		String ret = "";
		for (int i = 0; i < b.length; i++) {
			ret += String.format("\\x%02x", b[i]);
		}
		return ret;
	}
	
	public byte []retrieveId(String s) throws IOException{
		byte []sBytes = s.getBytes();
		byte []md5Hash = mDigest.digest(sBytes);
		
		Get g = new Get(md5Hash);
		g.addColumn(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
		
		HTableInterface table = con.getTable(HBPrefixMatchSchema.STRING2ID+schemaSuffix);
		Result r = table.get(g);
		byte []id = r.getValue(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
		if (id == null){
			System.err.println("Id does not exist for: "+s);
		}
		
		return id;
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
	
	final private byte []buildRangeScanKeyFromQuad(HBaseGeneric []quad, RowLimitPair limitPair) throws IOException, NumericalRangeException, ElementNotFoundException
	{
		currentPattern = "";
		spocOffsetMap.clear();
		
		buildTriplePattern(quad, limitPair);
		currentTableIndex = patternInfo.get(currentPattern).tableIndex;
		int keySize = patternInfo.get(currentPattern).prefixSize;
		byte []key = new byte[keySize];
		
		ArrayList<Get> string2IdGets = buildString2IdGets(quad, key);	
		
		//Query the String2Id table
		
		//if (numericalElement != null && keySize==TypedId.SIZE){//we have only a numerical in our key
			//Bytes.putBytes(key, HBPrefixMatchSchema.OFFSETS[currentTableIndex][2], numericalElement, 0, numericalElement.length);
		//}
		//else{
			key = buildRangeScanKeyFromMappedIds(string2IdGets, key);
		//}
		
		return key;
	}

	private void buildTriplePattern(HBaseGeneric[] quad, RowLimitPair limitPair) {
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

	final private byte[] buildRangeScanKeyFromMappedIds(ArrayList<Get> string2IdGets, byte []key) throws ElementNotFoundException, IOException {
		//long start = System.currentTimeMillis();
		HTableInterface table = con.getTable(HBPrefixMatchSchema.STRING2ID+schemaSuffix);
		Result []results = table.get(string2IdGets);
		table.close();
		//string2IdOverhead += System.currentTimeMillis()-start;
		
		for (Result result : results) {
			byte[] value = result.getValue(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
			if (value == null) {
				throw new ElementNotFoundException("Quad element not found: " + new String(result.toString()) + "\n" 
												+ (result.getRow() == null ? null : hexaString(result.getRow())));
			}

			int spocIndex = spocOffsetMap.get(new ByteArray(result.getRow()));
			int offset = HBPrefixMatchSchema.OFFSETS[currentTableIndex][spocIndex];

			if (spocIndex == OBJECT_POSITION)
				Bytes.putBytes(key, offset + 1, value, 0, value.length);
			else
				Bytes.putBytes(key, offset, value, 0, value.length);
		}	
		
		/*if (numericalElement != null){
			Bytes.putBytes(key, HBPrefixMatchSchema.OFFSETS[currentTableIndex][OBJECT_POSITION], numericalElement, 0, numericalElement.length);
		}*/
		return key;
	}

	private ArrayList<Get> buildString2IdGets(HBaseGeneric[] quad, byte []key) throws UnsupportedEncodingException, NumericalRangeException {
		ArrayList<Get> string2IdGets = new ArrayList<Get>();
		
		for (int i = 0; i < quad.length; i++) {
			if (quad[i] != null) {
				boundElements.add(quad[i]);
				int offset = HBPrefixMatchSchema.OFFSETS[currentTableIndex][i];
				
				if (quad[i] instanceof ValueWrapper) {
					Value val = ((ValueWrapper) quad[i]).getValue();

					byte[] sBytes;
					if (i != OBJECT_POSITION) {// not Object
						sBytes = val.toString().getBytes("UTF-8");
					} else {// Object
						if (val instanceof Literal) {// literal
							Literal l = (Literal) val;
							if (l.getDatatype() != null) {
								try {
									TypedId id = TypedId.createNumerical(l);
									Bytes.putBytes(key, offset, id.getBytes(), 0, TypedId.SIZE);
									continue;
								} catch (NonNumericalException e) {
									//TODO ??
								}
							}
							String lString = l.toString();
							sBytes = lString.getBytes("UTF-8");
						} else {// bNode or URI
							sBytes = val.toString().getBytes("UTF-8");
						}
					}

					byte[] md5Hash = mDigest.digest(sBytes);
					Get g = new Get(md5Hash);
					g.addColumn(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
					string2IdGets.add(g);
					spocOffsetMap.put(new ByteArray(md5Hash), i);
				}
				else if (quad[i] instanceof IdWrapper) {
					Id id = ((IdWrapper) quad[i]).getId();

					if (i != OBJECT_POSITION) {
						Bytes.putBytes(key, offset, id.getBytes(), 0,
								BaseId.SIZE);
					} else {// OBJECT position
						if (id instanceof TypedId
								&& ((TypedId) id).getType() == TypedId.NUMERICAL) {
							Bytes.putBytes(key, offset, id.getBytes(), 0, TypedId.SIZE);
						} else {
							Bytes.putBytes(key, offset + 1, id.getBytes(), 0, BaseId.SIZE);
						}
					}
				}
			}
		}
		
		return string2IdGets;
	}
	
	final private void doRangeScan(byte[] prefix, RowLimitPair limits, byte mapIdSwitch) throws IOException {
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

		parseRangeScanResults(prefix.length, results, mapIdSwitch);
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

	@Override
	public void populateTables(ArrayList<Statement> statements)
			throws Exception {
		HBaseLoader.load(con, schemaSuffix, statements);
	}

	@Override
	public long countResults(Value[] quad, long hardLimit) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}
	
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

	@Override
	public ArrayList<ArrayList<Id>> getResults(HBaseGeneric[] quad)
			throws IOException {		
		return getResults(quad, null);
	}

	@Override
	public ArrayList<ArrayList<Id>> getResults(HBaseGeneric[] quad, RowLimitPair limits) throws IOException {
		try {
			byte[] keyPrefix = buildRangeScanKeyFromQuad(quad, limits);
			
			//do the range scan
			if (limits!=null){
				doRangeScan(keyPrefix, limits, MAP_IDS_OFF);
			}
			else{
				doRangeScan(keyPrefix, MAP_IDS_OFF);
			}
			
			//build the results in SPOC order using only Id elements
			return null;
		} catch (NumericalRangeException e) {
			throw new IOException( "Bound numerical variable not in expected range: " + e.getMessage());
		} catch (ElementNotFoundException e) {
			throw new IOException(e.getMessage());
		}
	}

	@Override
	public ArrayList<ArrayList<Value>> getResults(Value[] quad)
			throws IOException {
		convertQuadToGenericPattern(quad);
		return getMaterializedResults(genericPattern);
	}
	
	public HBaseGeneric[] convertQuadToGenericPattern(Value[] quad) {
		for (int i = 0; i < quad.length; i++) {
			if (quad[i] != null){
				valuePattern[i].setValue(quad[i]);
				genericPattern[i] = valuePattern[i]; 
			}
		}
		return genericPattern;
	}
	
}
