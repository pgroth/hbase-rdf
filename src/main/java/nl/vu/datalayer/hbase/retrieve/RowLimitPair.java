package nl.vu.datalayer.hbase.retrieve;

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


	public RowLimitPair(TypedId limit, byte whichLimit) {
		switch (whichLimit){
		case START_LIMIT: {
			this.startLimit = limit;
			this.endLimit = new TypedId(limit.getNumericalType(), Bytes.toBytes(Long.MAX_VALUE));
		}
		case END_LIMIT:{
			this.startLimit = new TypedId(limit.getNumericalType(), Bytes.toBytes(Long.MIN_VALUE));
			this.endLimit = limit;
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
