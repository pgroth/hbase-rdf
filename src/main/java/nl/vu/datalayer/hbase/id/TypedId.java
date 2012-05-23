package nl.vu.datalayer.hbase.id;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.hadoop.hbase.util.Bytes;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.XMLSchema;

/**
 * 
 *
 */
public class TypedId extends BaseId{
	
	public static final byte STRING = 0;
	public static final byte NUMERICAL = 1;	
	
	public static final int XSD_DOUBLE = 0;
	public static final int XSD_DECIMAL = 1;
	public static final int XSD_INTEGER = 2;
	public static final int XSD_BOOLEAN = 3;
	public static final int XSD_NON_POSITIVE_INTEGER = 4;
	public static final int XSD_NEGATIVE_INTEGER = 5;
	public static final int XSD_LONG = 6;
	public static final int XSD_INT = 7;
	public static final int XSD_SHORT = 8;
	public static final int XSD_BYTE = 9;
	public static final int XSD_NON_NEGATIVE_INTEGER = 10;
	public static final int XSD_UNSIGNED_LONG = 11;
	public static final int XSD_UNSIGNED_INT = 12;
	public static final int XSD_UNSIGNED_SHORT = 13;
	public static final int XSD_UNSIGNED_BYTE = 14;
	public static final int XSD_POSITIVE_INTEGER = 15;
	
	public static final int SIZE = 9;
	
	public TypedId(int partitionId, long rowCount) {
		super(partitionId, rowCount);
		
		// first bit 0 - STRING
		byte []first =  new byte[1];
		id = Bytes.add(first, id);
	}
	
	public TypedId(){
		super();
	}
	
	public TypedId(int numericalType, byte []content) {
		super();
		
		//first bit 1 - NUMERICAL
		//next 4 bits - numerical type
		id = content;
		id[0] = (byte)((numericalType<<3) | 0x80);
	}
	
	/**
	 * @param l
	 * @return
	 * @throws NumericalRangeException
	 */
	public static TypedId createNumerical(Literal l) throws NumericalRangeException{
		URI datatype = l.getDatatype();
		
		byte []idSpace = new byte[SIZE];//prepare space also for storing the first byte
		
		if (datatype.equals(XMLSchema.DOUBLE)){
			Bytes.putDouble(idSpace, 1, l.doubleValue());
			return new TypedId(XSD_DOUBLE, idSpace);
		}
		else if (datatype.equals(XMLSchema.INT)){
			Bytes.putInt(idSpace, 1, l.intValue());
			return new TypedId(XSD_INT, idSpace);
		}
		else if (datatype.equals(XMLSchema.UNSIGNED_INT)){
			//Java does not have unsigned types
			//so we neet to use the next larger type - for unsigned int -> long
			long t = l.longValue();
			if (t < 0){
				throw new NumericalRangeException("unsignedInt does not accept negative values");
			}
			Bytes.putLong(idSpace, 1, t);
			return new TypedId(XSD_UNSIGNED_INT, idSpace);
		}
		else if (datatype.equals(XMLSchema.LONG)){
			Bytes.putLong(idSpace, 1, l.longValue());
			return new TypedId(XSD_LONG, idSpace);
		}
		else if(datatype.equals(XMLSchema.UNSIGNED_LONG)){
			//for unsigned long - we need to use BigInteger
			BigInteger val = l.integerValue();
			if (val.signum() == -1){
				throw new NumericalRangeException("unsignedLong does not accept negative values");
			}
			byte []valBytes = val.toByteArray();
			if (valBytes.length < SIZE-1){
				Bytes.putLong(valBytes, 1, val.longValue());
			}
			else{
				Bytes.putBytes(idSpace, 1, valBytes, SIZE-8, 8);
				idSpace[1] &= 0x7f;//maintain sign
			}
			
			return new TypedId(XSD_UNSIGNED_LONG, idSpace);
		}
		else if (datatype.equals(XMLSchema.SHORT)){
			Bytes.putShort(idSpace, 1, l.shortValue());
			return new TypedId(XSD_SHORT, idSpace);
		}
		else if (datatype.equals(XMLSchema.UNSIGNED_SHORT)){
			int us = l.intValue();
			if (us < 0){
				throw new NumericalRangeException("unsignedShort does not accept negative values");
			}
			Bytes.putInt(idSpace, 1, us);
			
			return new TypedId(XSD_SHORT, idSpace);
		}
		else if (datatype.equals(XMLSchema.BYTE)){
			Bytes.putByte(idSpace, 1, l.byteValue());
			return new TypedId(XSD_BYTE, idSpace);
		}
		else if (datatype.equals(XMLSchema.UNSIGNED_BYTE)){
			short ub = l.shortValue();
			if (ub < 0){
				throw new NumericalRangeException("unsignedByte does not accept negative values");
			}
			Bytes.putShort(idSpace, 1, ub);
			return new TypedId(XSD_UNSIGNED_BYTE, idSpace);
		}
		else if (datatype.equals(XMLSchema.FLOAT)){//we are using double instead of float in order
			double doubleValue = (double)l.floatValue();//to have the same representation
			Bytes.putDouble(idSpace, 1, doubleValue);
			return new TypedId(XSD_DOUBLE, idSpace);
		}
		else if (datatype.equals(XMLSchema.INTEGER)){
			BigInteger b = l.integerValue();
			byte []backingArray = b.toByteArray();
			int startOffset = backingArray.length-8;
			if (startOffset < 0){
				Bytes.putLong(backingArray, 1, b.longValue());
			}
			else
				Bytes.putBytes(idSpace, 1, backingArray, startOffset, SIZE-1);
			
			return new TypedId(XSD_INTEGER, idSpace);
		}
		else if (datatype.equals(XMLSchema.NON_POSITIVE_INTEGER)){
			BigInteger b = l.integerValue();
			if (b.signum() == 1){
				throw new NumericalRangeException("nonPositiveInteger does not accept positive values");
			}
			
			byte []backingArray = b.toByteArray();
			int startOffset = backingArray.length-8;
			if (startOffset < 0){
				Bytes.putLong(idSpace, 1, b.longValue());
			}
			else{
				Bytes.putBytes(idSpace, 1, backingArray, startOffset, SIZE-1);
				if (b.signum() == -1)//maintain sign 
					idSpace[1] |= (byte)0x80;
			}
			
			return new TypedId(XSD_NON_POSITIVE_INTEGER, idSpace);
		}
		else if (datatype.equals(XMLSchema.NEGATIVE_INTEGER)){
			BigInteger b = l.integerValue();
			if (b.signum() != -1){
				throw new NumericalRangeException("negativeInteger does not accept non-negative values");
			}
			
			byte []backingArray = b.toByteArray();
			int startOffset = backingArray.length-8;
			if (startOffset < 0){
				Bytes.putLong(idSpace, 1, b.longValue());
			}
			else{
				Bytes.putBytes(idSpace, 1, backingArray, startOffset, SIZE-1);
				idSpace[1] |= (byte)0x80; //should always be negative so maintain sign bit
			}
			
			return new TypedId(XSD_NEGATIVE_INTEGER, idSpace);
		}
		else if (datatype.equals(XMLSchema.NON_NEGATIVE_INTEGER)){
			BigInteger b = l.integerValue();
			if (b.signum() == -1){
				throw new NumericalRangeException("nonNegativeInteger does not accept negative values");
			}
			
			byte []backingArray = b.toByteArray();
			int startOffset = backingArray.length-8;
			if (startOffset < 0){
				Bytes.putLong(idSpace, 1, b.longValue());
			}
			else{
				Bytes.putBytes(idSpace, 1, backingArray, startOffset, SIZE-1);
				idSpace[1] &= (byte)0x7f; //maintain positive
			}
			
			return new TypedId(XSD_NON_NEGATIVE_INTEGER, idSpace);
		}
		else if (datatype.equals(XMLSchema.POSITIVE_INTEGER)){
			BigInteger b = l.integerValue();
			if (b.signum() != 1){
				throw new NumericalRangeException("positiveInteger does not accept non-positive values");
			}
			
			byte []backingArray = b.toByteArray();
			int startOffset = backingArray.length-8;
			if (startOffset < 0){
				Bytes.putLong(idSpace, 1, b.longValue());
			}
			else{
				Bytes.putBytes(idSpace, 1, backingArray, startOffset, SIZE-1);
				idSpace[1] &= (byte)0x7f; //maintain positive
			}
			
			return new TypedId(XSD_POSITIVE_INTEGER, idSpace);
		}
		else if (datatype.equals(XMLSchema.DECIMAL)){//2 bytes scale; 6 bytes unscaled
			Bytes.putShort(idSpace, 1, (short)l.decimalValue().scale());
			byte []backingArray = l.decimalValue().unscaledValue().toByteArray();
			int startOffset = backingArray.length-6;
			int length = 6;
			if (startOffset < 0){
				length += startOffset;
				startOffset = 0;
			}
			Bytes.putBytes(idSpace, SIZE-length, backingArray, startOffset, length);
			
			return new TypedId(XSD_DECIMAL, idSpace);
		}
		else
			return null;
	}
	

	public TypedId(byte[] id) {
		super(id);
	}

	public byte getType() {
		return (byte)(id[0]>>7 & 1);
	}
	
	public static byte getType(byte type){
		return (byte)(type>>7 & 1);
	}

	public byte getNumericalType() {
		return (byte)(id[0]>>3 & 0x0f);
	}
	
	public byte []getContent(){
		return Bytes.tail(id, 8);
	}

	@Override
	public String toString() {
		if (getType() == STRING){
			return new String(getContent());
		}
		else{//NUMERICAL
			byte numType = getNumericalType();
			
			switch(numType){
			case XSD_DOUBLE:{
				double d = Bytes.toDouble(id, 1);
				return Double.toString(d);
			}
			case XSD_DECIMAL:{
				int scale = (int)Bytes.toShort(id, 1);
			    byte[] bigDBytes = new byte[6];
			    System.arraycopy(id, 1 + Bytes.SIZEOF_SHORT, bigDBytes, 0, SIZE-1-Bytes.SIZEOF_SHORT);
			    BigDecimal num =  new BigDecimal(new BigInteger(bigDBytes), scale);
				return num.toPlainString();
			}
			case XSD_INTEGER: case XSD_LONG: case XSD_UNSIGNED_INT: {
				long l = Bytes.toLong(id, 1);
				return Long.toString(l);
			}
			case XSD_BOOLEAN:{
				boolean b = (id[1] != (byte)0);
				return Boolean.toString(b);
			}
			case XSD_NON_POSITIVE_INTEGER: case XSD_NEGATIVE_INTEGER: case XSD_NON_NEGATIVE_INTEGER: case XSD_UNSIGNED_LONG: case XSD_POSITIVE_INTEGER:{
				BigInteger b = new BigInteger(getContent());
				return b.toString();
			}
			case XSD_INT: case XSD_UNSIGNED_SHORT: {
				int i = Bytes.toInt(id, 1);
				return Integer.toString(i);
			}
			case XSD_SHORT: case XSD_UNSIGNED_BYTE: {
				short s = Bytes.toShort(id, 1);
				return Short.toString(s);
			}
			case XSD_BYTE: {
				byte b = id[1];
				return Byte.toString(b);
			}
			}
		}
		return super.toString();
	}
}
