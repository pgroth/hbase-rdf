package nl.vu.datalayer.hbase.id;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author sever
 *
 */
public class DataPair implements Writable{
	
	private Id id;
	private byte position;
	
	public static final byte S=0;
	public static final byte P=1;
	public static final byte O=2;
	public static final byte C=3;
	
	public DataPair() {
		super();
		id = null;
	}	

	public DataPair(Id id, byte position) {
		super();
		this.id = id;
		this.position = position;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readByte();
		byte []idBytes = new byte[size];
		
		in.readFully(idBytes);
		id = Id.build(idBytes);
		
		position = in.readByte();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(id.getBytes().length);
		out.write(id.getBytes());
		
		out.writeByte(position);
	}

	public Id getId() {
		return id;
	}

	public byte getPosition() {
		return position;
	}
	
	public void setId(Id id) {
		this.id = id;
	}

	public String toString(){
		String ret = id.toString()+" : ";
		ret += position;
		return ret;
	}

	@Override
	public boolean equals(Object obj) {
		DataPair pair = (DataPair)obj;
		return (id.equals(pair.getId()) && position == pair.getPosition());
	}
	
	
}
