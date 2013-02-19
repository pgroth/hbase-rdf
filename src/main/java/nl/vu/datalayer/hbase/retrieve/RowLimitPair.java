package nl.vu.datalayer.hbase.retrieve;

import org.apache.hadoop.hbase.util.Bytes;

public class RowLimitPair {
	public static final byte START_LIMIT = 0;
	public static final byte END_LIMIT = 1;
	
	private byte []startLimit;
	private byte []endLimit;
	
	public RowLimitPair(byte[] startLimit, byte[] endLimit) {
		this.startLimit = startLimit;
		this.endLimit = endLimit;
	}
	
	public RowLimitPair(byte[] limit, byte whichLimit) {
		switch (whichLimit){
		case START_LIMIT: {
			this.startLimit = limit;
			this.endLimit = Bytes.toBytes(Long.MAX_VALUE);
		}
		case END_LIMIT:{
			this.startLimit = Bytes.toBytes(Long.MIN_VALUE);
			this.endLimit = limit;
		}
		default: throw new RuntimeException("Unknown limit type");
		}
	}

	public byte[] getStartLimit() {
		return startLimit;
	}

	public byte[] getEndLimit() {
		return endLimit;
	}	

}
