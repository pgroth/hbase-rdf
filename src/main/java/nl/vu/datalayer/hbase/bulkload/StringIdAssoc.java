package nl.vu.datalayer.hbase.bulkload;

import java.io.IOException;

import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class StringIdAssoc {
	
	public static final String STRING2ID_DIR = "/string2Id";
	public static final String ID2STRING_DIR = "/id2String";
	
	public static class Id2StringMapper extends 
			org.apache.hadoop.mapreduce.Mapper<ImmutableBytesWritable, Text, ImmutableBytesWritable, Put>
	{
		@Override
		protected void map(ImmutableBytesWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			byte []keyBytes = new byte [key.getLength()];
			System.arraycopy(key.get(), key.getOffset(), keyBytes, 0, key.getLength());
			
			Put outVal = new Put(keyBytes);
			
			byte []valueBytes = new byte [value.getLength()];
			System.arraycopy(value.getBytes(), 0, valueBytes, 0, value.getLength());
			outVal.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, valueBytes);
			
			context.write(key, outVal);
		}
	}
	
	/**
	 * Hash function to convert any string to an 8-byte integer
	 * @param key
	 * @return
	 */
	private static long hashFunction(String key){
		long hash = 0;
		
		for (int i = 0; i < key.length(); i++) {
			hash = (int)key.charAt(i) + (hash << 6) + (hash << 16) - hash;
		}
		
		return hash;
	}
	
	public static byte []reverseString(byte []sourceBytes, int sourceLength){
		int lastIndex;
		if (sourceBytes[sourceLength-1] == '\"')//eliminate last " for literals in order to have a good dispersion
			lastIndex = sourceLength-2;
		else
			lastIndex = sourceLength-1;
		
		byte []outValueBytes = new byte[lastIndex+1];
		for (int i = 0; i < outValueBytes.length; i++) {
			outValueBytes[i] = sourceBytes[lastIndex - i];
		}
		return outValueBytes;
	}
	
	public static class String2IdMapper extends 
			org.apache.hadoop.mapreduce.Mapper<ImmutableBytesWritable, Text, ImmutableBytesWritable, Put> 
	{
		@Override
		protected void map(ImmutableBytesWritable inKey, Text inValue, Context context) 
										throws IOException, InterruptedException {
			byte []inValueBytes;
			byte []sourceBytes = inValue.getBytes();
			int sourceLength = inValue.getLength();
			
			if (sourceLength > HConstants.MAX_ROW_LENGTH){//if the key is too long use the hash
				String s = Text.decode(sourceBytes, 0, sourceLength);
				long hash = hashFunction(s);
				inValueBytes = new byte[9];
				//the first byte is left 00, because it is not part of RDF characters
				Bytes.putLong(inValueBytes, 1, hash);
			}
			else{//reverse the value so that we have a better dispersion for links which have the same prefix		
				inValueBytes = reverseString(sourceBytes, sourceLength);
			}		
			ImmutableBytesWritable outKey = new ImmutableBytesWritable(inValueBytes);				

			Put outVal = new Put(inValueBytes);
			byte []inKeyBytes = new byte [inKey.getLength()];
			System.arraycopy(inKey.get(), inKey.getOffset(), inKeyBytes, 0, inKey.getLength());
			outVal.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, inKeyBytes);

			context.write(outKey, outVal);
		}
	}
}
