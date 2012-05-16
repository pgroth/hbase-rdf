package nl.vu.datalayer.hbase.id;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author sever
 *
 */
public class BaseId extends Id implements WritableComparable<BaseId> {
	
	public static int SIZE = 8;
	
	/**
	 * @param partitionId
	 * @param localCounter
	 */
	public BaseId(int partitionId, long localCounter) {
		super();
		long tripleID = ((long)partitionId)<<24;
		tripleID |= localCounter & 0x0000000000ffffffL;
		id = Bytes.toBytes(tripleID);
	}
	
	public BaseId(byte[] id) {
		super(id);
	}
	
	public BaseId(){
		super();
	}

	@Override
	public int hashCode() {
		return Bytes.toInt(id, id.length-4, 4);
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
	public int compareTo(BaseId other) {		
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
		BaseId otherId = (BaseId)obj;
		byte []otherBytes = otherId.getBytes();
		
		if (id.length != otherBytes.length)
			return false;
		
		for (int i = 0; i < otherBytes.length; i++) {
			if (id[i] != otherBytes[i])
				return false;
		}
		
		return true;
	}
	
	public String toString(){
		String ret = "";
		for (int i = 0; i < id.length; i++) {
			ret += String.format("%02x ", id[i]);
		}
		return ret;
	}
}
