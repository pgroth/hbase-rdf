package nl.vu.datalayer.hbase.bulkload;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import nl.vu.datalayer.hbase.id.BaseId;
import nl.vu.datalayer.hbase.id.HBaseValue;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class StringIdAssoc {
	
	public static final String STRING2ID_DIR = "/string2Id";
	public static final String ID2STRING_DIR = "/id2String";
	
	public static class Id2StringMapper extends 
			org.apache.hadoop.mapreduce.Mapper<ImmutableBytesWritable, HBaseValue, ImmutableBytesWritable, Put>
	{
		private ByteArrayOutputStream out = new ByteArrayOutputStream();
		private DataOutputStream dataOut = new DataOutputStream(out);
		private byte [] keyBytes = new byte [BaseId.SIZE];//
		
		public static long getMapOutputRecordSizeEstimate() {
			long immutableByesKey = BaseId.SIZE+Bytes.SIZEOF_INT+Bytes.SIZEOF_LONG;
			long putSize = 155;//empirically determined
			return (immutableByesKey+putSize);
		}
		
		@Override
		protected void map(ImmutableBytesWritable key, HBaseValue value, Context context) throws IOException, InterruptedException {
			System.arraycopy(key.get(), key.getOffset(), keyBytes, 0, key.getLength());	
			Put outVal = new Put(keyBytes);
	
			out.reset();
			value.write(dataOut);
			
			outVal.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, out.toByteArray());
			context.write(key, outVal);
		}
	}
	
	
	
	public static class String2IdMapper extends 
			org.apache.hadoop.mapreduce.Mapper<ImmutableBytesWritable, HBaseValue, ImmutableBytesWritable, Put> 
	{
		private static final int HASH_SIZE = 16;
		private MessageDigest mDigest;
		private byte [] inKeyBytes = new byte [BaseId.SIZE];
		private ImmutableBytesWritable outKey = new ImmutableBytesWritable();
		
		public static long getMapOutputRecordSizeEstimate() {
			long immutableByesKey = HASH_SIZE+Bytes.SIZEOF_INT+Bytes.SIZEOF_LONG;
			long putSize = 111;//empirically determined
			return (immutableByesKey+putSize);
		}
		
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
			outKey.set(md5HashBytes);				

			Put outVal = new Put(md5HashBytes);
			
			System.arraycopy(inKey.get(), inKey.getOffset(), inKeyBytes, 0, inKey.getLength());
			outVal.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, inKeyBytes);

			context.write(outKey, outVal);
		}
	}
}
