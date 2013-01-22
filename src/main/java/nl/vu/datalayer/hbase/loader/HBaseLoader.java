package nl.vu.datalayer.hbase.loader;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.connection.NativeJavaConnection;
import nl.vu.datalayer.hbase.exceptions.NonNumericalException;
import nl.vu.datalayer.hbase.exceptions.NumericalRangeException;
import nl.vu.datalayer.hbase.id.BaseId;
import nl.vu.datalayer.hbase.id.Id;
import nl.vu.datalayer.hbase.id.TypedId;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;

public class HBaseLoader {
	
	public static final String DEFAULT_CONTEXT = "http://DEFAULT_CONTEXT";
	private static long idCounter;
	private static HashMap<Value, ValueIdPair> dictionary;
	private static List<Put> spocTableData;
	
	//assuming all triples can fit into memory
	public static void load(HBaseConnection con, String schemaSuffix, ArrayList<Statement> statements) throws Exception{
		if (!(con instanceof NativeJavaConnection)){
			throw new Exception("Expected NativeJavaConnection upon loading");
		}
		
		idCounter = 0;//assuming no triples loaded before
		dictionary = new HashMap<Value, HBaseLoader.ValueIdPair>();
		spocTableData = new ArrayList<Put>(statements.size());
		
		quadBreakDown(statements);	
		
		loadDictionaryTables(con, schemaSuffix);
			
		loadIndexTables(con, schemaSuffix);
	}

	private static void loadIndexTables(HBaseConnection con, String schemaSuffix)
			throws IOException {
		HTableInterface spocTable = con.getTable(HBPrefixMatchSchema.TABLE_NAMES[HBPrefixMatchSchema.SPOC]+schemaSuffix);
		HTableInterface pocsTable = con.getTable(HBPrefixMatchSchema.TABLE_NAMES[HBPrefixMatchSchema.POCS]+schemaSuffix);
		HTableInterface ospcTable = con.getTable(HBPrefixMatchSchema.TABLE_NAMES[HBPrefixMatchSchema.OSPC]+schemaSuffix);
		
		spocTable.put(spocTableData);
		for (Put put : spocTableData) {
			Put pocsPut = build(25, 0, 8, 17, put.getRow());
			pocsTable.put(pocsPut);
			
			Put ospcPut = build(9, 17, 0, 25, put.getRow());
			ospcTable.put(ospcPut);
		}
	}

	private static void loadDictionaryTables(HBaseConnection con,
			String schemaSuffix) throws NoSuchAlgorithmException, IOException,
			UnsupportedEncodingException {
		MessageDigest mDigest = MessageDigest.getInstance("MD5");
		
		HTableInterface string2IdTable = con.getTable(HBPrefixMatchSchema.STRING2ID+schemaSuffix);
		HTableInterface id2StringTable = con.getTable(HBPrefixMatchSchema.ID2STRING+schemaSuffix);
		for (Map.Entry<Value, ValueIdPair> entry : dictionary.entrySet()) {
			ValueIdPair valueIdPair = entry.getValue();
			Value val = valueIdPair.value;
			byte []valBytes = val.toString().getBytes("UTF-8");
			byte []md5Hash = mDigest.digest(valBytes);
			
			Put string2IdPut = new Put(md5Hash);
			string2IdPut.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, valueIdPair.id.getBytes());
			string2IdTable.put(string2IdPut);
			
			Put id2StringPut = new Put(valueIdPair.id.getBytes());
			id2StringPut.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, valBytes);
			id2StringTable.put(id2StringPut);
		}
	}

	private static void quadBreakDown(ArrayList<Statement> statements)
			throws NumericalRangeException {
		for (Statement statement : statements) {
			Value subject = statement.getSubject();
			Id subjectId = generateId(idCounter, subject);
			Value predicate = statement.getPredicate();
			Id predicateId  = generateId(idCounter, predicate);
			
			Value object = statement.getObject();
			Id objectId;
			if (object instanceof Literal){
				Literal l = (Literal)object;
				if (l.getDatatype() == null){//the Literals with no datatype are considered Strings
					objectId = generateId(idCounter, object);
				}
				else{//we have a datatype
					try {
						objectId = TypedId.createNumerical(l);
					} catch (NonNumericalException e) {
						objectId  = generateId(idCounter, object);
					}
				}
			}
			else{
				objectId = generateId(idCounter, object);
			}
			
			Value context = statement.getContext();
			if (context == null){
				context = new URIImpl(DEFAULT_CONTEXT);
			}
			Id contextId = generateId(idCounter, context);
			
			byte []spoc = Bytes.add(Bytes.add(subjectId.getBytes(), predicateId.getBytes(), objectId.getBytes()), contextId.getBytes());
			Put spocPut = new Put(spoc);
			spocPut.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, null);
			spocTableData.add(spocPut);
		}
	}
	
	public static Put build(int sOffset, int pOffset, int oOffset, int cOffset, byte []source) throws IOException
	{
		byte []outBytes = new byte[HBPrefixMatchSchema.KEY_LENGTH];
		
		Bytes.putBytes(outBytes, sOffset, source, 0, 8);//put S 
		Bytes.putBytes(outBytes, pOffset, source, 8, 8);//put P 
		Bytes.putBytes(outBytes, oOffset, source, 16, 9);//put O
		Bytes.putBytes(outBytes, cOffset, source, 25, 8);//put C
		
		Put put = new Put(outBytes);
		put.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, null);
		
		return put;
	}
	
	public static Id generateId(long oldCounter, Value value){
		Id newId = new BaseId(oldCounter);
		ValueIdPair valueIdPair = new ValueIdPair(value, newId);
		
		dictionary.put(value, valueIdPair);
		
		idCounter = oldCounter+1;
		return newId;
	}
	
	
	static class ValueIdPair{
		public Value value;
		public Id id;
		
		public ValueIdPair(Value value, Id id) {
			super();
			this.value = value;
			this.id = id;
		}
	}

}
