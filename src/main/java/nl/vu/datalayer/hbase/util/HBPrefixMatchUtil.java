package nl.vu.datalayer.hbase.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.exceptions.ElementNotFoundException;
import nl.vu.datalayer.hbase.exceptions.NumericalRangeException;
import nl.vu.datalayer.hbase.id.BaseId;
import nl.vu.datalayer.hbase.id.HBaseValue;
import nl.vu.datalayer.hbase.id.TypedId;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
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
public class HBPrefixMatchUtil implements IHBaseUtil {
	
	private HBaseConnection con;
	
	/**
	 * Maps the query patterns to the associated tables that resolve those patterns 
	 */
	private HashMap<String, Integer> pattern2Table;
	
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
	private HashMap<ByteArray, Value> id2ValueMap;
	
	private ArrayList<ArrayList<ByteArray>> quadResults;
	
	private ArrayList<Value> boundElements;
	
	private ValueFactory valueFactory;
	
	private String schemaSuffix;
	
	private MessageDigest mDigest;

	private int keySize;

	private byte [] numericalElement;

	private String currentPattern;

	private HashMap<ByteArray, Integer> spocOffsetMap = new HashMap<ByteArray, Integer>();

	public HBPrefixMatchUtil(HBaseConnection con) {
		super();
		this.con = con;
		pattern2Table = new HashMap<String, Integer>(16);
		buildPattern2TableHashMap();
		id2ValueMap = new HashMap<ByteArray, Value>();
		quadResults = new ArrayList<ArrayList<ByteArray>>();
		boundElements = new ArrayList<Value>();
		valueFactory = new ValueFactoryImpl();
		
		Properties prop = new Properties();
		try{
			prop.load(new FileInputStream("config.properties"));
		}
		catch (IOException e) {
			//continue to use the default properties
		}
		schemaSuffix = prop.getProperty(HBPrefixMatchSchema.SUFFIX_PROPERTY, "");	
		try {
			mDigest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	
	private void buildPattern2TableHashMap(){
		pattern2Table.put("????", HBPrefixMatchSchema.SPOC);
		pattern2Table.put("|???", HBPrefixMatchSchema.SPOC);
		pattern2Table.put("||??", HBPrefixMatchSchema.SPOC);
		pattern2Table.put("|||?", HBPrefixMatchSchema.SPOC);
		pattern2Table.put("||||", HBPrefixMatchSchema.SPOC);
		
		pattern2Table.put("?|??", HBPrefixMatchSchema.POCS);
		pattern2Table.put("?||?", HBPrefixMatchSchema.POCS);
		pattern2Table.put("?|||", HBPrefixMatchSchema.POCS);
		
		pattern2Table.put("??|?", HBPrefixMatchSchema.OSPC);
		pattern2Table.put("|?|?", HBPrefixMatchSchema.OSPC);
		
		pattern2Table.put("??||", HBPrefixMatchSchema.OCSP);
		pattern2Table.put("|?||", HBPrefixMatchSchema.OCSP);
		
		pattern2Table.put("???|", HBPrefixMatchSchema.CSPO);
		pattern2Table.put("|??|", HBPrefixMatchSchema.CSPO);
		pattern2Table.put("||?|", HBPrefixMatchSchema.CSPO);
		
		pattern2Table.put("?|?|", HBPrefixMatchSchema.CPSO);
	}

	public ArrayList<ArrayList<String>> getRow(String[] quad){
		return null;
	}
	
	
	@Override
	public ArrayList<ArrayList<Value>> getResults(Value[] quad)
			throws IOException {//we expect the pattern in the order SPOC	
		try {
			retrievalInit();
			
			long start = System.currentTimeMillis();
			byte[] startKey = buildRangeScanKeyFromQuad(quad);
			long keyBuildOverhead = System.currentTimeMillis()-start;

			long startSearch = System.currentTimeMillis();
			ArrayList<Get> batchIdGets = doRangeScan(startKey);
			long searchTime = System.currentTimeMillis() - startSearch;

			start = System.currentTimeMillis();
			Result[] id2StringResults = doBatchId2String(batchIdGets);
			id2StringOverhead = System.currentTimeMillis()-start;
			
			System.out.println("Search time: "+searchTime+"; Id2StringOverhead: "+id2StringOverhead+"; String2IdOverhead: "
									+string2IdOverhead+"; KeyBuildOverhead: "+keyBuildOverhead);
			
			updateId2ValueMap(id2StringResults);			
			
			return buildSPOCOrderResults();
		
		} catch (NumericalRangeException e) {
			System.err.println("Bound variable numerical not in expected range: "+e.getMessage());
			return null;
		} catch (ElementNotFoundException e) {
			System.err.println(e.getMessage());
			return null;
		}
	}

	final private ArrayList<ArrayList<Value>> buildSPOCOrderResults() {
		int queryElemNo = boundElements.size();
		ArrayList<ArrayList<Value>> ret = new ArrayList<ArrayList<Value>>();
		
		for (ArrayList<ByteArray> quadList : quadResults) {
			ArrayList<Value> newQuadResult = new ArrayList<Value>(4);
			newQuadResult.addAll(Arrays.asList(new Value[]{null, null, null, null}));			
			
			fillInUnboundElements(queryElemNo, quadList, newQuadResult);		
			fillInBoundElements(newQuadResult);
			
			ret.add(newQuadResult);
		}
		return ret;
	}

	final private void fillInBoundElements(ArrayList<Value> newQuadResult) {
		for (int i = 0, j = 0; i<newQuadResult.size() && j<boundElements.size(); i++) {
			if (newQuadResult.get(i) == null){
				newQuadResult.set(i, boundElements.get(j++));
			}
		}
	}

	final private void fillInUnboundElements(int queryElemNo, ArrayList<ByteArray> quadList, ArrayList<Value> newQuadResult) {
		for (int i = 0; i < quadList.size(); i++) {
			if ((i+queryElemNo) == HBPrefixMatchSchema.ORDER[currentTableIndex][2] && 
					TypedId.getType(quadList.get(i).getBytes()[0]) == TypedId.NUMERICAL){
				//handle numericals
				TypedId id = new TypedId(quadList.get(i).getBytes());
				newQuadResult.set(2, id.toLiteral());
			}
			else{
				newQuadResult.set(HBPrefixMatchSchema.TO_SPOC_ORDER[currentTableIndex][(i+queryElemNo)], 
						id2ValueMap.get(quadList.get(i)));
			}
		}
	}

	final private void updateId2ValueMap(Result[] id2StringResults) throws IOException, ElementNotFoundException {
		HBaseValue hbaseValue = new HBaseValue();
		for (Result result : id2StringResults) {
			byte []rowVal = result.getValue(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
			byte []rowKey = result.getRow();
			if (rowVal == null || rowKey == null){
				throw new ElementNotFoundException("Id not found in Id2String table: "+(rowKey == null ? null : hexaString(rowKey)));
			}
			else{
				ByteArrayInputStream byteStream = new ByteArrayInputStream(rowVal);
				hbaseValue.readFields(new DataInputStream(byteStream));
				Value val = hbaseValue.getUnderlyingValue();
				id2ValueMap.put(new ByteArray(rowKey), val);
			}
		}
	}

	final private Result[] doBatchId2String(ArrayList<Get> batchIdGets) throws IOException {
		HTableInterface id2StringTable = con.getTable(HBPrefixMatchSchema.ID2STRING+schemaSuffix);
		Result []id2StringResults = id2StringTable.get(batchIdGets);
		id2StringTable.close();
		return id2StringResults;
	}

	final private ArrayList<Get> doRangeScan(byte[] startKey) throws IOException {
		Filter prefixFilter = new PrefixFilter(startKey);
		Filter keyOnlyFilter = new KeyOnlyFilter();
		Filter filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, prefixFilter, keyOnlyFilter);
		
		Scan scan = new Scan(startKey, filterList);
		scan.setCaching(100);

		String tableName = HBPrefixMatchSchema.TABLE_NAMES[currentTableIndex]+schemaSuffix;
		System.out.println("Retrieving from table: "+tableName);
		
		HTableInterface table = con.getTable(tableName);
		ResultScanner results = table.getScanner(scan);

		ArrayList<Get> batchGets = parseRangeScanResults(startKey.length, results);
		table.close();
		return batchGets;
	}

	final private ArrayList<Get> parseRangeScanResults(int startKeyLength, ResultScanner results) throws IOException {
		Result r = null;
		int sizeOfInterest = HBPrefixMatchSchema.KEY_LENGTH - startKeyLength;

		ArrayList<Get> batchGets = new ArrayList<Get>();
		while ((r = results.next()) != null) {
			parseKey(r.getRow(), startKeyLength, sizeOfInterest, batchGets);
		}
		results.close();
		return batchGets;
	}

	final private void retrievalInit() {
		id2ValueMap.clear();
		quadResults.clear();
		boundElements.clear();
		id2StringOverhead = 0;
		string2IdOverhead = 0;
	}
	
	/*public Value convertStringToValue(String val){
		if (val.startsWith("_:")){//bNode
			return valueFactory.createBNode(val.substring(2));
		}
		else if (val.startsWith("\"")){//literal
			int lastDQuote = val.lastIndexOf("\"");
			if (lastDQuote == val.length()-1){
				return valueFactory.createLiteral(val.substring(1, val.length()-1));
			}
			else{
				String label = val.substring(1, lastDQuote-1);
				if (val.charAt(lastDQuote+1) == '@'){//we have a language
					String language = val.substring(lastDQuote+2);				
					return valueFactory.createLiteral(label, language);
				}
				else if (val.charAt(lastDQuote+1) == '^'){
					String dataType = val.substring(lastDQuote+4, val.length()-1);
					return valueFactory.createLiteral(label, valueFactory.createURI(dataType));
				}
				else 
					throw new IllegalArgumentException("Literal not in proper format");
			}
		}else{//URIs
			return valueFactory.createURI(val);
		}
	}*/
	
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
	
	private void parseKey(byte []key, int startIndex, int sizeOfInterest, ArrayList<Get> batchGets) throws IOException{
		
		int elemNo = sizeOfInterest/BaseId.SIZE;
		ArrayList<ByteArray> currentQuad = new ArrayList<ByteArray>(elemNo);
		
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
					ByteArray newElem = new ByteArray(elemKey);
					currentQuad.add(newElem);
					continue;
				}
			}
			else{//for non-Object positions
				length = BaseId.SIZE;
				elemKey = new byte[length];
				System.arraycopy(key, crtIndex, elemKey, 0, length);
			}
			
			ByteArray newElem = new ByteArray(elemKey);
			currentQuad.add(newElem);
			
			if (id2ValueMap.get(newElem) == null){
				id2ValueMap.put(newElem, null);
				Get g = new Get(elemKey);
				g.addColumn(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
				batchGets.add(g);
			}
			
			crtIndex += length;
		}
		
		quadResults.add(currentQuad);
	}
	
	final private byte []buildRangeScanKeyFromQuad(Value []quad) throws IOException, NumericalRangeException, ElementNotFoundException
	{
		currentPattern = "";
		keySize = 0;
		spocOffsetMap.clear();
		
		ArrayList<Get> string2IdGets = buildString2IdGets(quad);	
		currentTableIndex = pattern2Table.get(currentPattern);
		
		//Query the String2Id table
		long start = System.currentTimeMillis();
		HTableInterface table = con.getTable(HBPrefixMatchSchema.STRING2ID+schemaSuffix);
		Result []results = table.get(string2IdGets);
		table.close();
		string2IdOverhead += System.currentTimeMillis()-start;
		
		return buildRangeScanKeyFromResults(results);
	}

	final private byte[] buildRangeScanKeyFromResults(Result[] results) throws ElementNotFoundException {
		byte []key = new byte[keySize];
		for (Result result : results) {
			
			byte []value = result.getValue(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
			if (value == null){
				throw new ElementNotFoundException("Quad element could not be found: "+new String(result.getRow())+"\n"+hexaString(result.getRow()));
			}
			
			int spocIndex = spocOffsetMap.get(new ByteArray(result.getRow()));
			int offset = HBPrefixMatchSchema.OFFSETS[currentTableIndex][spocIndex];
			
			if (spocIndex == 2)
				Bytes.putBytes(key, offset+1, value, 0, value.length);
			else
				Bytes.putBytes(key, offset, value, 0, value.length);
		}	
		
		if (numericalElement != null){
			Bytes.putBytes(key, HBPrefixMatchSchema.OFFSETS[currentTableIndex][2], numericalElement, 0, numericalElement.length);
		}
		return key;
	}

	private ArrayList<Get> buildString2IdGets(Value[] quad) throws UnsupportedEncodingException, NumericalRangeException {
		ArrayList<Get> string2IdGets = new ArrayList<Get>();
		
		numericalElement = null;
		for (int i = 0; i < quad.length; i++) {
			if (quad[i] == null)
				currentPattern += "?";
			else{
				currentPattern += "|";
				boundElements.add(quad[i]);
				
				byte []sBytes;
				if (i != 2){//not Object
					keySize += BaseId.SIZE;		
					sBytes = quad[i].toString().getBytes("UTF-8");
				}
				else{//Object
					keySize += TypedId.SIZE;
					if (quad[i] instanceof Literal){//literal
						Literal l = (Literal)quad[i];
						if (l.getDatatype() != null){
							TypedId id = TypedId.createNumerical(l);
							if (id != null){
								numericalElement = id.getBytes();
								continue;
							}
						}
						String lString = l.toString();
						sBytes = lString.getBytes("UTF-8");					
					}
					else{//bNode or URI
						sBytes = quad[i].toString().getBytes("UTF-8");
					}
				}
				
				//byte []reverseString = StringIdAssoc.reverseBytes(sBytes, sBytes.length);
				byte []md5Hash = mDigest.digest(sBytes);
				Get g = new Get(md5Hash);
				g.addColumn(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
				string2IdGets.add(g);
				spocOffsetMap.put(new ByteArray(md5Hash), i);
			}
		}
		
		return string2IdGets;
	}

	@Override
	public String getRawCellValue(String subject, String predicate,
			String object) throws IOException {
		return null;
	}

	@Override
	public void populateTables(ArrayList<Statement> statements)
			throws Exception {

	}

}
