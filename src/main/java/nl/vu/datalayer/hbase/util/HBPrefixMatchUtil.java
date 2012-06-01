package nl.vu.datalayer.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import nl.vu.datalayer.hbase.bulkload.StringIdAssoc;
import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.id.BaseId;
import nl.vu.datalayer.hbase.id.NumericalRangeException;
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
	private int tableIndex;
	
	/**
	 * Variable storing time for the overhead of id2StringMap mappings upon retrieval
	 */
	private long id2StringOverhead = 0;
	
	/**
	 * Variable storing time for the overhead of String2Id mappings 
	 */
	private long string2IdOverhead = 0;
	
	private HashMap<ByteArray, Value> id2ValueMap;
	
	private ArrayList<ArrayList<ByteArray>> quadResults;
	
	private ArrayList<Value> boundElements;
	
	private ValueFactory valueFactory;

	public HBPrefixMatchUtil(HBaseConnection con) {
		super();
		this.con = con;
		pattern2Table = new HashMap<String, Integer>(16);
		buildHashMap();
		id2ValueMap = new HashMap<ByteArray, Value>();
		quadResults = new ArrayList<ArrayList<ByteArray>>();
		boundElements = new ArrayList<Value>();
		valueFactory = new ValueFactoryImpl();
	}
	
	private void buildHashMap(){
		pattern2Table.put("????", HBPrefixMatchSchema.SPOC);
		pattern2Table.put("|???", HBPrefixMatchSchema.SPOC);
		pattern2Table.put("||??", HBPrefixMatchSchema.SPOC);
		pattern2Table.put("|||?", HBPrefixMatchSchema.SPOC);
		pattern2Table.put("||||", HBPrefixMatchSchema.SPOC);
		
		pattern2Table.put("?|??", HBPrefixMatchSchema.POCS);
		pattern2Table.put("?||?", HBPrefixMatchSchema.POCS);
		pattern2Table.put("?|||", HBPrefixMatchSchema.POCS);
		
		pattern2Table.put("??|?", HBPrefixMatchSchema.OCSP);
		pattern2Table.put("??||", HBPrefixMatchSchema.OCSP);
		pattern2Table.put("|?||", HBPrefixMatchSchema.OCSP);
		
		pattern2Table.put("|?|?", HBPrefixMatchSchema.OSPC);
		
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
			id2ValueMap.clear();
			quadResults.clear();
			boundElements.clear();
			id2StringOverhead = 0;
			byte[] startKey = buildKey(quad);
			if (startKey == null) {
				return new ArrayList<ArrayList<Value>>();
			}

			//start search in the quad tables
			long startSearch = System.currentTimeMillis();
			Filter prefixFilter = new PrefixFilter(startKey);
			Filter keyOnlyFilter = new KeyOnlyFilter();
			Filter filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, prefixFilter, keyOnlyFilter);
			
			Scan scan = new Scan(startKey, filterList);
			scan.setCaching(100);

			String tableName = HBPrefixMatchSchema.TABLE_NAMES[tableIndex];
			HTableInterface table = con.getTable(tableName);
			ResultScanner results = table.getScanner(scan);

			Result r = null;
			int sizeOfInterest = HBPrefixMatchSchema.KEY_LENGTH - startKey.length;
			HTableInterface id2StringTable = con.getTable(HBPrefixMatchSchema.ID2STRING);

			ArrayList<Get> batchGets = new ArrayList<Get>();
			while ((r = results.next()) != null) {
				parseKey(r.getRow(), startKey.length, sizeOfInterest, batchGets);
			}
			results.close();
			long searchTime = System.currentTimeMillis() - startSearch;

			long start = System.currentTimeMillis();
			Result []id2StringResults = id2StringTable.get(batchGets);
			id2StringOverhead = System.currentTimeMillis()-start;
			
			//System.out.println("Search time: "+searchTime+"; Id2StringOverhead: "+id2StringOverhead+"; String2IdOverhead: "+string2IdOverhead);
			
			//update the internal mapping between ids and strings
			for (Result result : id2StringResults) {
				byte []rowVal = result.getValue(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
				byte []rowKey = result.getRow();
				if (rowVal == null || rowKey == null){
					System.err.println("Id not found: "+(rowKey == null ? null : hexaString(rowKey)));
				}
				else{
					Value val = convertStringToValue(new String(rowVal));
					id2ValueMap.put(new ByteArray(rowKey), val);
				}
			}			
			
			int queryElemNo = boundElements.size();
			// build the quads in SPOC order using strings
			ArrayList<ArrayList<Value>> ret = new ArrayList<ArrayList<Value>>();
			
			for (ArrayList<ByteArray> quadList : quadResults) {
				//ArrayList<String> newList = new ArrayList<String>(quadList.size());
				
				ArrayList<Value> newQuadResult = new ArrayList<Value>(4);
				newQuadResult.addAll(Arrays.asList(new Value[]{null, null, null, null}));			
				
				//fill in unbound elements
				for (int i = 0; i < quadList.size(); i++) {
					if ((i+queryElemNo) == HBPrefixMatchSchema.ORDER[tableIndex][2] && 
							TypedId.getType(quadList.get(i).getBytes()[0]) == TypedId.NUMERICAL){
						//handle numerical
						TypedId id = new TypedId(quadList.get(i).getBytes());
						newQuadResult.set(2, id.toLiteral());
					}
					else{
						newQuadResult.set(HBPrefixMatchSchema.TO_SPOC_ORDER[tableIndex][(i+queryElemNo)], id2ValueMap.get(quadList.get(i)));
					}
				}
					
				//fill in bound elements
				for (int i = 0, j = 0; i<newQuadResult.size() && j<boundElements.size(); i++) {
					if (newQuadResult.get(i) == null){
						newQuadResult.set(i, boundElements.get(j++));
					}
				}
				
				ret.add(newQuadResult);
			}
			
			return ret;
			
		} catch (NumericalRangeException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public Value convertStringToValue(String val){
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
		byte []key = StringIdAssoc.reverseBytes(sBytes, sBytes.length);
		
		Get g = new Get(key);
		g.addColumn(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
		
		HTableInterface table = con.getTable(HBPrefixMatchSchema.STRING2ID);
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
			if (crtIndex == HBPrefixMatchSchema.OFFSETS[tableIndex][2]){//for the Object position
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
	
	private byte []buildKey(Value []quad) throws IOException, NumericalRangeException
	{
		String pattern = "";
		int keySize = 0;
		
		ArrayList<Get> string2Ids = new ArrayList<Get>();
		ArrayList<Integer> offsets = new ArrayList<Integer>();
		byte []numerical = null;
		for (int i = 0; i < quad.length; i++) {
			if (quad[i] == null)
				pattern += "?";
			else{
				pattern += "|";
				boundElements.add(quad[i]);
				
				byte []sBytes;
				if (i != 2){//not Object
					keySize += BaseId.SIZE;		
					sBytes = quad[i].toString().getBytes();
				}
				else{
					keySize += TypedId.SIZE;
					if (quad[i] instanceof Literal){//literal
						Literal l = (Literal)quad[i];
						if (l.getDatatype() != null){
							TypedId id = TypedId.createNumerical(l);
							if (id != null){
								numerical = id.getBytes();
								continue;
							}
						}
						String lString = l.toString();
						sBytes = lString.getBytes();					
					}
					else{
						String elem = quad[i].toString();
						sBytes = elem.getBytes();
					}
				}
				
				byte []reverseString = StringIdAssoc.reverseBytes(sBytes, sBytes.length);
				Get g = new Get(reverseString);
				g.addColumn(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
				string2Ids.add(g);
				offsets.add(i);
			}
		}
		
		tableIndex = pattern2Table.get(pattern);
		
		//HTable table = con.getTable(HBPrefixMatchSchema.STRING2ID);
		HTableInterface table = con.getTable(HBPrefixMatchSchema.STRING2ID);
		
		byte []key = new byte[keySize];
		for (int i=0; i<string2Ids.size(); i++) {
			Get get = string2Ids.get(i);
			
			long start = System.currentTimeMillis();
			Result result = table.get(get);
			byte []value = result.getValue(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
			string2IdOverhead += System.currentTimeMillis()-start;
			
			if (value == null){
				byte []rowKey = result.getRow();
				System.err.println("Quad element could not be found "+(rowKey == null ? null : hexaString(rowKey)));
				return null;
			}
			
			int spocIndex = offsets.get(i);
			int offset = HBPrefixMatchSchema.OFFSETS[tableIndex][spocIndex];
			
			if (spocIndex == 2)
				Bytes.putBytes(key, offset+1, value, 0, value.length);
			else
				Bytes.putBytes(key, offset, value, 0, value.length);
		}	
		if (numerical != null){
			Bytes.putBytes(key, HBPrefixMatchSchema.OFFSETS[tableIndex][2], numerical, 0, numerical.length);
		}
		
		return key;
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
