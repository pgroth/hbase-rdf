package nl.vu.datalayer.hbase.bulkload;

import java.io.IOException;

import nl.vu.datalayer.hbase.id.BaseId;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PrefixMatch {
	
	private static byte [] outBytes = new byte[HBPrefixMatchSchema.KEY_LENGTH];
	private static ImmutableBytesWritable outKey = new ImmutableBytesWritable();
	
	public static long getMapOutputRecordSizeEstimate() {
		long immutableBytesWritableSize = Bytes.SIZEOF_INT+HBPrefixMatchSchema.KEY_LENGTH+Bytes.SIZEOF_LONG;
		long putSize = 120;//empirically determined
		return (immutableBytesWritableSize+putSize);
	}

	public static class PrefixMatchSPOCMapper extends 
		org.apache.hadoop.mapreduce.Mapper<NullWritable, ImmutableBytesWritable, ImmutableBytesWritable, Put>
	{
		@Override
		protected void map(NullWritable key, ImmutableBytesWritable value, Context context)
				throws IOException, InterruptedException {
			//we assume we get the quads in SPOC order
			ImmutableBytesWritable outKey = value;
			
			Put outValue = new Put(value.get());
			outValue.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, null);
			
			context.write(outKey, outValue);
		}		
	}
	
	public static void build(int sOffset, int pOffset, int oOffset, int cOffset, byte []source, Context context) throws IOException, InterruptedException
	{
		Bytes.putBytes(outBytes, sOffset, source, 0, 8);//put S 
		Bytes.putBytes(outBytes, pOffset, source, 8, 8);//put P 
		Bytes.putBytes(outBytes, oOffset, source, 16, 9);//put O
		Bytes.putBytes(outBytes, cOffset, source, 25, 8);//put C
		
		outKey.set(outBytes);
		
		Put outValue = new Put(outBytes);
		outValue.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, null);
		
		context.write(outKey, outValue);
	}
	
	public static class PrefixMatchPOCSMapper extends 
			org.apache.hadoop.mapreduce.Mapper<NullWritable, ImmutableBytesWritable, ImmutableBytesWritable, Put>
	{
		@Override
		protected void map(NullWritable key, ImmutableBytesWritable value, Context context)
				throws IOException, InterruptedException {
			//we assume we get the quads in SPOC order
			build(25, 0, 8, 17, value.get(), context);
		}		
	}
	
	public static class PrefixMatchOCSPMapper extends 
			org.apache.hadoop.mapreduce.Mapper<NullWritable, ImmutableBytesWritable, ImmutableBytesWritable, Put>
	{
		@Override
		protected void map(NullWritable key, ImmutableBytesWritable value, Context context)
				throws IOException, InterruptedException {
			//we assume we get the quads in SPOC order
			
			build(17, 25, 0, 9, value.get(), context);
		}	
	}
	
	public static class PrefixMatchCSPOMapper
			extends org.apache.hadoop.mapreduce.Mapper<NullWritable, ImmutableBytesWritable, ImmutableBytesWritable, Put> {
		
		@Override
		protected void map(NullWritable key, ImmutableBytesWritable value, Context context)
				throws IOException, InterruptedException {
			// we assume we get the quads in SPOC order
			
			build(8, 16, 24, 0, value.get(), context);
		}
	}

	public static class PrefixMatchCPSOMapper extends 
			org.apache.hadoop.mapreduce.Mapper<NullWritable, ImmutableBytesWritable, ImmutableBytesWritable, Put> {

		@Override
		protected void map(NullWritable key, ImmutableBytesWritable value, Context context) throws IOException, InterruptedException {
			// we assume we get the quads in SPOC order

			build(16, 8, 24, 0, value.get(), context);
			
		}
	}
	
	public static class PrefixMatchOSPCMapper extends 
			org.apache.hadoop.mapreduce.Mapper<NullWritable, ImmutableBytesWritable, ImmutableBytesWritable, Put> {

		@Override
		protected void map(NullWritable key, ImmutableBytesWritable value, Context context) throws IOException, InterruptedException {
			// we assume we get the quads in SPOC order

			build(9, 17, 0, 25, value.get(), context);
		}
	}
}
