package nl.vu.datalayer.hbase.util;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

import nl.vu.datalayer.hbase.HBaseConnection;
import nl.vu.datalayer.hbase.bulkload.StringIdAssoc;
import nl.vu.datalayer.hbase.id.BaseId;
import nl.vu.datalayer.hbase.id.NumericalRangeException;
import nl.vu.datalayer.hbase.id.TypedId;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.ntriples.NTriplesUtil;

public class HBPrefixMatchUtil implements IHBaseUtil {
	
	private HBaseConnection con;
	
	private HashMap<String, Integer> pattern2Table;
	
	private int tableIndex;

	public HBPrefixMatchUtil(HBaseConnection con) {
		super();
		this.con = con;
		pattern2Table = new HashMap<String, Integer>(16);
		buildHashMap();
	}
	
	private void buildHashMap(){
		pattern2Table.put("????", HBPrefixMatchSchema.SPOC);
		pattern2Table.put("|???", HBPrefixMatchSchema.SPOC);
		pattern2Table.put("||??", HBPrefixMatchSchema.SPOC);
		pattern2Table.put("|||?", HBPrefixMatchSchema.SPOC);
		
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

	@Override
	public ArrayList<ArrayList<String>> getRow(String[] quad)
			throws IOException {//we expect the pattern in the order SPOC
		
		//int tableIndex = getTable(quad);
		
		try {
			byte[] startKey = buildKey(quad);

			if (startKey == null) {
				return new ArrayList<ArrayList<String>>();
			}

			BigInteger endKeyBigInt = new BigInteger(1, startKey);//the last byte might be 0xff so we use BigInteger
			byte[] endKey = endKeyBigInt.add(BigInteger.valueOf(1)).toByteArray(); //to handle wrap up

			//System.arraycopy(startKey, 0, endKey, 0, startKey.length);

			Scan scan = new Scan(startKey, Bytes.padHead(endKey, startKey.length - endKey.length));
			scan.addColumn(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
			scan.setCaching(500);

			String tableName = HBPrefixMatchSchema.TABLE_NAMES[tableIndex];
			System.out.println(tableName);
			HTable table = new HTable(con.getConfiguration(), tableName);
			ResultScanner results = table.getScanner(scan);

			Result r = null;
			int sizeOfInterest = HBPrefixMatchSchema.KEY_LENGTH - startKey.length;
			HTable id2StringTable = new HTable(con.getConfiguration(), HBPrefixMatchSchema.ID2STRING);
			ArrayList<ArrayList<String>> ret = new ArrayList<ArrayList<String>>();

			while ((r = results.next()) != null) {
				ArrayList<String> elems = parseKey(r.getRow(), startKey.length, sizeOfInterest, tableIndex, id2StringTable);
				ret.add(elems);
			}
			results.close();

			/*
			 * Result []finalResults = id2StringTable.get(id2StringGets); int
			 * elementsPerResult = sizeOfInterest/8;
			 * 
			 * 
			 * ArrayList<String> current = new
			 * ArrayList<String>(elementsPerResult); for (int i = 0; i <
			 * finalResults.length; i++) { String value = new
			 * String(finalResults
			 * [i].getValue(HBPrefixMatchSchema.COLUMN_FAMILY,
			 * HBPrefixMatchSchema.COLUMN_NAME)); current.add(value);
			 * 
			 * if ((i+1)%elementsPerResult == 0){ ret.add(current); current =
			 * new ArrayList<String>(elementsPerResult); } }
			 */
			return ret;
			
		} catch (NumericalRangeException e) {
			e.printStackTrace();
			return null;
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
		byte []key = StringIdAssoc.reverseString(sBytes, sBytes.length);
		
		Get g = new Get(key);
		g.addColumn(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
		
		HTable table = new HTable(con.getConfiguration(), HBPrefixMatchSchema.STRING2ID);
		Result r = table.get(g);
		byte []id = r.getValue(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
		if (id == null){
			System.err.println("Id does not exist for: "+s);
		}
		
		return id;
	}
	
	private ArrayList<String> parseKey(byte []key, int startIndex, int sizeOfInterest, int tableIndex, HTable id2StringTable) throws IOException{
		
		int elemNo = sizeOfInterest/8;
		ArrayList<String> ret = new ArrayList<String>(elemNo);
		
		int crtIndex = startIndex;
		for (int i = 0; i < elemNo; i++) {
			int length;
			byte [] elemKey;
			if (crtIndex == HBPrefixMatchSchema.OFFSETS[tableIndex][2]){//for the Object position
				length = TypedId.SIZE;
				if (TypedId.getType(key[crtIndex]) == TypedId.STRING){
					elemKey = new byte[8];
					System.arraycopy(key, crtIndex+1, elemKey, 0, 8);
				}
				else{
					elemKey = new byte[9];
					System.arraycopy(key, crtIndex, elemKey, 0, 9);
					TypedId id = new TypedId(elemKey);
					ret.add(id.toString());
					crtIndex += length;
					continue;
				}
			}
			else{//for non-Object positions
				length = BaseId.SIZE;
				elemKey = new byte[length];
				System.arraycopy(key, crtIndex, elemKey, 0, length);
			}
			 
			Get g = new Get(elemKey);
			g.addColumn(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
			
			//map IDs back to associated Strings
			Result res = id2StringTable.get(g);
			byte []id = res.getValue(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
			if (id == null){
				System.err.println("Could not find id: "+hexaString(elemKey));
			}
			else{
				String elem = new String(id);
				ret.add(elem);
			}
			
			crtIndex += length;
		}
		
		return ret;
	}
	
	private byte []buildKey(String []quad) throws IOException, NumericalRangeException
	{
		String pattern = "";
		int keySize = 0;
		
		ArrayList<Get> string2Ids = new ArrayList<Get>();
		ArrayList<Integer> offsets = new ArrayList<Integer>();
		byte []numerical = null;
		for (int i = 0; i < quad.length; i++) {
			if (quad[i].equals("?"))
				pattern += "?";
			else{
				pattern += "|";
				
				if (i != 2)//not Object
					keySize += 8;
				else{
					keySize += 9;
					if (quad[i].startsWith("\"")){
						Literal l = NTriplesUtil.parseLiteral(quad[i], new ValueFactoryImpl());
						if (l.getDatatype() != null){
							TypedId id = TypedId.createNumerical(l);
							if (id != null){
								numerical = id.getBytes();
								continue;
							}
						}
					}
				}
				
				byte []sBytes = quad[i].getBytes();
				byte []reverseString = StringIdAssoc.reverseString(sBytes, sBytes.length);
				Get g = new Get(reverseString);
				g.addColumn(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
				string2Ids.add(g);
				offsets.add(i);
			}
		}
		
		tableIndex = pattern2Table.get(pattern);
		HTable table = new HTable(con.getConfiguration(), HBPrefixMatchSchema.STRING2ID);
		
		byte []key = new byte[keySize];
		for (int i=0; i<string2Ids.size(); i++) {
			Get get = string2Ids.get(i);
			Result result = table.get(get);
			
			byte []value = result.getValue(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME);
			if (value == null){
				System.err.println("Quad element could not be found");
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void populateTables(ArrayList<Statement> statements)
			throws Exception {
		// TODO Auto-generated method stub

	}

}
