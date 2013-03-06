package nl.vu.datalayer.hbase.id;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.datalayer.hbase.retrieve.HBaseGeneric;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;

public abstract class Id implements WritableComparable<Id>, HBaseGeneric {
	
	public static final byte BASE_ID = 0;
	public static final byte TYPED_ID = 1;
	
	protected byte []id;
	
	public Id() {
		id = null;
	}

	public Id(byte[] id) {
		this.id = id;
	}
	
	public static Id build(byte []idBytes){
		switch (idBytes.length){
			case 8:
				return new BaseId(idBytes); 
			case 9:
				return new TypedId(idBytes);
			default:
				throw new RuntimeException("Unexpected id length");
		}
	}
	
	public static Id build(long content, byte type){
		switch (type){
			case BASE_ID:
				return new BaseId(content); 
			case TYPED_ID:
				return new TypedId(content);
			default:
				throw new RuntimeException("Unexpected id length");
		}
	}

	public byte[] getBytes()
	{
		return id;
	}
	
	public abstract byte[] getContent();
	
	public abstract int getContentOffset();

	public void set(byte[] content) {
		this.id = content;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readByte();
		id = new byte[size];
		
		in.readFully(id);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(id.length);
		out.write(id);
	}

	@Override
	public int compareTo(Id other) {		
		byte []otherBytes = other.getBytes();
		
		if (id.length < otherBytes.length)
			return -1;
		else if (id.length > otherBytes.length)
			return 1;
		
		for (int i = 0; i < otherBytes.length; i++) {
			if (id[i] < otherBytes[i])
				return -1;
			else if (id[i] > otherBytes[i])
				return 1;
		}
		
		return 0;
	}

	@Override
	public boolean equals(Object obj) {
		Id otherId = (Id)obj;
		byte []otherBytes = otherId.getBytes();
		
		if (id.length != otherBytes.length)
			return false;
		
		for (int i = 0; i < otherBytes.length; i++) {
			if (id[i] != otherBytes[i])
				return false;
		}
		
		return true;
	}

	@Override
	public int hashCode() {//used for partitioning
		return Bytes.toInt(id, id.length-4, 4);
	}
}
