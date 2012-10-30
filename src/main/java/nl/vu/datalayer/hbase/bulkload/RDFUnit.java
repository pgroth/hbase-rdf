package nl.vu.datalayer.hbase.bulkload;

public class RDFUnit {
	public static final byte QUAD = 0;
	public static final byte TRIPLE = 1;

	public static byte getNumberOfAtoms(byte type){
		switch (type){
		case QUAD: return 4;
		case TRIPLE: return 3;
		default: return 0;
		}
	}
	
	public static int getAverageUnitSize(byte type){
		switch (type){
		case QUAD: return 234;
		case TRIPLE: return 160;
		default: return 0;
		}
	}
}
