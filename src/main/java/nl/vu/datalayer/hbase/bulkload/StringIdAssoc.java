package nl.vu.datalayer.hbase.bulkload;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import nl.vu.datalayer.hbase.id.HBaseValue;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class StringIdAssoc {
	
	public static final String STRING2ID_DIR = "/string2Id";
	public static final String ID2STRING_DIR = "/id2String";
	
	public static class Id2StringMapper extends 
			org.apache.hadoop.mapreduce.Mapper<ImmutableBytesWritable, HBaseValue, ImmutableBytesWritable, Put>
	{
		@Override
		protected void map(ImmutableBytesWritable key, HBaseValue value, Context context) throws IOException, InterruptedException {
			
			byte []keyBytes = new byte [key.getLength()];
			System.arraycopy(key.get(), key.getOffset(), keyBytes, 0, key.getLength());
			
			Put outVal = new Put(keyBytes);
			
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			value.write(new DataOutputStream(out));
			//byte []valueBytes = new byte [value.getLength()];
			//System.arraycopy(value.getBytes(), 0, valueBytes, 0, value.getLength());
			
			outVal.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, out.toByteArray());
			
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
	
	public static byte []reverseBytes(byte []sourceBytes){
		int lastIndex = sourceBytes.length-1;
		
		byte []outValueBytes = new byte[lastIndex+1];
		for (int i = 0; i < outValueBytes.length; i++) {
			outValueBytes[i] = sourceBytes[lastIndex - i];
		}
		return outValueBytes;
	}
	
	public static class String2IdMapper extends 
			org.apache.hadoop.mapreduce.Mapper<ImmutableBytesWritable, HBaseValue, ImmutableBytesWritable, Put> 
	{
		private MessageDigest mDigest;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			try {
				mDigest = MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}

		@Override
		protected void map(ImmutableBytesWritable inKey, HBaseValue inValue, Context context) 
										throws IOException, InterruptedException {
			
			byte []md5HashBytes = mDigest.digest(inValue.toString().getBytes("UTF-8"));
			ImmutableBytesWritable outKey = new ImmutableBytesWritable(md5HashBytes);				

			Put outVal = new Put(md5HashBytes);
			byte []inKeyBytes = new byte [inKey.getLength()];
			System.arraycopy(inKey.get(), inKey.getOffset(), inKeyBytes, 0, inKey.getLength());
			outVal.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, inKeyBytes);

			context.write(outKey, outVal);
		}
	}
}
