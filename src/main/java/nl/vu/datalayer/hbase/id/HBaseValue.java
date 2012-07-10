package nl.vu.datalayer.hbase.id;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;
import nl.vu.datalayer.hbase.util.HBPrefixMatchUtil;

import org.apache.hadoop.io.WritableComparable;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;

public class HBaseValue implements WritableComparable<HBaseValue> {
	
	public static final byte URI_TYPE = 0;
	public static final byte BNODE_TYPE = 1;
	public static final byte LITERAL_TYPE = 2;
	
	public static final byte DATATYPE_LITERAL = 1;
	public static final byte LANGUAGE_LITERAL = 2;
	
	private Value value;

	public HBaseValue() {
		super();
	}

	public HBaseValue(Value value) {
		super();
		this.value = value;
	}
	
	@Override
	public int hashCode() {//used for partitioning
		return value.hashCode();
	}

	public Value getUnderlyingValue() {
		return value;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof HBaseValue)){
			return false;
		}
		
		HBaseValue val = (HBaseValue)obj;
		return value.equals(val.getUnderlyingValue());
	}

	@Override
	public int compareTo(HBaseValue o) {//order lexicographically
		return value.toString().compareTo(o.getUnderlyingValue().toString());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		byte type = in.readByte();
		byte baseType = (byte)(type & 0x3);
		
		switch (baseType){
		case URI_TYPE:{
			String s = in.readUTF();
			value = new URIImpl(s);
			break;
		}
		case BNODE_TYPE: {
			String s = in.readUTF();
			value = new BNodeImpl(s);
			break;
		}
		case LITERAL_TYPE:{
			int labelSize = in.readInt();
			byte []labelBytes = new byte[labelSize];
			in.readFully(labelBytes);
			String label = new String(labelBytes);
			
			byte literalType = (byte)(type>>2);
			
			switch (literalType){
			case DATATYPE_LITERAL:{
				String s = in.readUTF();
				URI datatype = new URIImpl(s);
				value = new LiteralImpl(label, datatype);
				break;
			}
			case LANGUAGE_LITERAL:{
				String lang = in.readUTF();
				value = new LiteralImpl(label, lang);
				break;
			}
			default:{//plain literal
				value = new LiteralImpl(label);
			}
			}
			break;
		}
		default: throw new RuntimeException("Unknow value base type");
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		byte type;
		if (value instanceof URI){
			type = URI_TYPE;
			out.write(type);
			out.writeUTF(value.stringValue());
		} else if (value instanceof BNode){
			type = BNODE_TYPE;
			out.write(type);
			out.writeUTF(value.stringValue());
		}
		else if (value instanceof Literal){
			type = LITERAL_TYPE;
			Literal l = (Literal)value;
			
			byte []labelBytes = l.stringValue().getBytes();
			if (l.getDatatype() != null){
				type |= DATATYPE_LITERAL<<2;
				
				out.write(type);
				out.writeInt(labelBytes.length);
				out.write(labelBytes);
				out.writeUTF(l.getDatatype().stringValue());
			}
			else if (l.getLanguage() != null){
				type |= LANGUAGE_LITERAL<<2;
				
				out.write(type);
				out.writeInt(labelBytes.length);
				out.write(labelBytes);
				out.writeUTF(l.getLanguage());
			}
			else{//plain literal
				out.write(type);
				out.writeInt(labelBytes.length);
				out.write(labelBytes);
			}
		}
	}

	@Override
	public String toString() {
		return value.toString();
	}
	
	public static void main(String[] args) {
		MessageDigest mDigest;
		try {
			mDigest = MessageDigest.getInstance("MD5");
			//byte []hash = mDigest.digest(new String("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").getBytes("UTF-8"));
			byte []hash = mDigest.digest(new String("http://people.w3.org/simon/foaf#i").getBytes("UTF-8"));
			System.out.println(HBPrefixMatchUtil.hexaString(hash));
			
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
