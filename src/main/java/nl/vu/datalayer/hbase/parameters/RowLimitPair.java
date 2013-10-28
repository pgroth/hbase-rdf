package nl.vu.datalayer.hbase.parameters;

import nl.vu.datalayer.hbase.id.TypedId;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * A class wrapping the limits which will be used to limit HBase scans
 * The limits are inclusive
 * 
 * @author Sever Fundatureanu
 *
 */
public class RowLimitPair {
	public static final byte START_LIMIT = 0;
	public static final byte END_LIMIT = 1;
	
	private TypedId startLimit;
	private TypedId endLimit;
	
	public RowLimitPair(byte[] startLimit, byte[] endLimit, byte numericalType) {
		this.startLimit = new TypedId(numericalType, startLimit);
		this.endLimit = new TypedId(numericalType, endLimit);
	}
	
	public RowLimitPair(TypedId startLimit, TypedId endLimit) {
		super();
		this.startLimit = startLimit;
		this.endLimit = endLimit;
	}


	public RowLimitPair(TypedId limit, byte whichLimit) {//TODO assuming positive integers
		byte []backingArray = new byte[TypedId.SIZE];
		switch (whichLimit){
		case START_LIMIT: {
			this.startLimit = limit;
			Bytes.putLong(backingArray, 1, Long.MAX_VALUE);
			this.endLimit = new TypedId(limit.getNumericalType(), backingArray);
			break;
		}
		case END_LIMIT:{
			Bytes.putLong(backingArray, 1, 0);
			this.startLimit = new TypedId(limit.getNumericalType(), backingArray);
			this.endLimit = limit;
			break;
		}
		default: throw new RuntimeException("Unknown limit type");
		}
	}

	public TypedId getStartLimit() {
		return startLimit;
	}

	public TypedId getEndLimit() {
		return endLimit;
	}

}
