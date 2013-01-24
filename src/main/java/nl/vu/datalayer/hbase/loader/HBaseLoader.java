package nl.vu.datalayer.hbase.loader;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
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
import nl.vu.datalayer.hbase.id.HBaseValue;
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
		HBaseValue hbaseValue = new HBaseValue();
		//we create the byte stream in advance to avoid reallocation for each result
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream(100);
		DataOutputStream dataOutputStream = new DataOutputStream(byteStream);
		
		
		HTableInterface string2IdTable = con.getTable(HBPrefixMatchSchema.STRING2ID+schemaSuffix);
		HTableInterface id2StringTable = con.getTable(HBPrefixMatchSchema.ID2STRING+schemaSuffix);
		for (Map.Entry<Value, ValueIdPair> entry : dictionary.entrySet()) {
			ValueIdPair valueIdPair = entry.getValue();
			Value val = valueIdPair.value;
			byte []valBytes = val.toString().getBytes("UTF-8");
			byte []md5Hash = mDigest.digest(valBytes);
			
			Put string2IdPut = new Put(md5Hash);
			string2IdPut.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, valueIdPair.id.getContent());
			string2IdTable.put(string2IdPut);
			
			byteStream.reset();
			hbaseValue.setValue(val);
			hbaseValue.write(dataOutputStream);
			byte []serializedValue = byteStream.toByteArray();
			
			Put id2StringPut = new Put(valueIdPair.id.getContent());
			id2StringPut.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, serializedValue);
			id2StringTable.put(id2StringPut);
		}
	}

	private static void quadBreakDown(ArrayList<Statement> statements)
			throws NumericalRangeException {
		for (Statement statement : statements) {
			Value subject = statement.getSubject();
			Id subjectId = generateId(idCounter, subject, Id.BASE_ID);
			Value predicate = statement.getPredicate();
			Id predicateId  = generateId(idCounter, predicate, Id.BASE_ID);
			
			Value object = statement.getObject();
			Id objectId;
			if (object instanceof Literal){
				Literal l = (Literal)object;
				if (l.getDatatype() == null){//the Literals with no datatype are considered Strings
					objectId = generateId(idCounter, object, Id.TYPED_ID);
				}
				else{//we have a datatype
					try {
						objectId = TypedId.createNumerical(l);
					} catch (NonNumericalException e) {
						objectId  = generateId(idCounter, object, Id.TYPED_ID);
					}
				}
			}
			else{
				objectId = generateId(idCounter, object, Id.TYPED_ID);
			}
			
			Value context = statement.getContext();
			if (context == null){
				context = new URIImpl(DEFAULT_CONTEXT);
			}
			Id contextId = generateId(idCounter, context, Id.BASE_ID);
			
			byte [] spoc = Bytes.add(Bytes.add(subjectId.getBytes(), predicateId.getBytes(), objectId.getBytes()), contextId.getBytes());
			spoc = buildSPOCKey(subjectId, predicateId, objectId, contextId);
			
			Put spocPut = new Put(spoc);
			spocPut.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, null);
			spocTableData.add(spocPut);
		}
	}

	private static byte[] buildSPOCKey(Id subjectId, Id predicateId,
											Id objectId, Id contextId) {
		byte []spoc = new byte[HBPrefixMatchSchema.KEY_LENGTH];
		System.arraycopy(subjectId.getBytes(), subjectId.getContentOffset(), spoc, 0, BaseId.SIZE);
		System.arraycopy(predicateId.getBytes(), predicateId.getContentOffset(), spoc, 8, BaseId.SIZE);
		if (objectId instanceof TypedId){
			System.arraycopy(objectId.getBytes(), 0, spoc, 16, TypedId.SIZE);
		}
		else{
			System.arraycopy(objectId.getBytes(), 0, spoc, 17, BaseId.SIZE);
		}
		System.arraycopy(contextId.getBytes(), contextId.getContentOffset(), spoc, 25, BaseId.SIZE);
		return spoc;
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
	
	public static Id generateId(long oldCounter, Value value, byte idType){
		ValueIdPair existingPair;
		Id retId;
		if ((existingPair = dictionary.get(value)) == null) {
			retId = Id.build(oldCounter, idType);
			ValueIdPair valueIdPair = new ValueIdPair(value, retId);

			dictionary.put(value, valueIdPair);
			idCounter = oldCounter + 1;
		}
		else{
			retId = existingPair.id;
		}
		
		return retId;
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
