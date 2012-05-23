package nl.vu.datalayer.hbase.bulkload;

import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

public class PrefixMatchTest {

	public static class PutMatcher extends ArgumentMatcher<Put> {
		private Put put;
		
	    public PutMatcher(Put put) {
			super();
			this.put = put;
		}

		public boolean matches(Object object) {
			Put p = (Put)object;
			byte []rowKey = p.getRow();
			byte []thisRowKey = put.getRow();
			if (rowKey.length != thisRowKey.length)
				return false;
			
			for (int i = 0; i < thisRowKey.length; i++) {
				if (rowKey[i] != thisRowKey[i])
					return false;
			}
			
			return true;
	    }
	 }

	
	@Test
	public void testSPOCMapper() {
		
		try {
			PrefixMatch.PrefixMatchSPOCMapper mapper = new PrefixMatch.PrefixMatchSPOCMapper();
			
			Context context = mock(Context.class);
			byte []valBytes = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
					0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
					0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
					0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38};
			ImmutableBytesWritable value = new ImmutableBytesWritable(valBytes);
			
			mapper.map(NullWritable.get(), value, context);
			
			ImmutableBytesWritable outKey = new ImmutableBytesWritable(valBytes);
			Put put = new Put(valBytes);
			put.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, null);
			
			verify(context).write(eq(outKey), argThat(new PutMatcher(put)));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
		
	
	@Test
	public void testPOCSMapper() {
		
		try {
			PrefixMatch.PrefixMatchPOCSMapper mapper = new PrefixMatch.PrefixMatchPOCSMapper();
			
			Context context = mock(Context.class);
			byte []valBytes = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
					0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
					0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
					0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38};
			ImmutableBytesWritable value = new ImmutableBytesWritable(valBytes);
			
			mapper.map(NullWritable.get(), value, context);
			
			byte []outBytes = {0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
					0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
					0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,
					0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
			
			ImmutableBytesWritable outKey = new ImmutableBytesWritable(outBytes);
			Put put = new Put(outBytes);
			put.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, null);
			
			verify(context).write(eq(outKey), argThat(new PutMatcher(put)));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void testOCSPMapper() {
		
		try {
			PrefixMatch.PrefixMatchOCSPMapper mapper = new PrefixMatch.PrefixMatchOCSPMapper();
			
			Context context = mock(Context.class);
			byte []valBytes = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
					0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
					0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
					0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38};
			ImmutableBytesWritable value = new ImmutableBytesWritable(valBytes);
			
			mapper.map(NullWritable.get(), value, context);
			
			byte []outBytes = {0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
					0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,
					0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
					0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18};
			
			ImmutableBytesWritable outKey = new ImmutableBytesWritable(outBytes);
			Put put = new Put(outBytes);
			put.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, null);
			
			verify(context).write(eq(outKey), argThat(new PutMatcher(put)));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void testCSPOMapper() {	
		try {
			PrefixMatch.PrefixMatchCSPOMapper mapper = new PrefixMatch.PrefixMatchCSPOMapper();
			
			Context context = mock(Context.class);
			byte []valBytes = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
					0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
					0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
					0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38};
			ImmutableBytesWritable value = new ImmutableBytesWritable(valBytes);
			
			mapper.map(NullWritable.get(), value, context);
			
			byte []outBytes = {0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,
					0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
					0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
					0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28};
			
			ImmutableBytesWritable outKey = new ImmutableBytesWritable(outBytes);
			Put put = new Put(outBytes);
			put.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, null);
			
			verify(context).write(eq(outKey), argThat(new PutMatcher(put)));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void testCPSOMapper() {	
		try {
			PrefixMatch.PrefixMatchCPSOMapper mapper = new PrefixMatch.PrefixMatchCPSOMapper();
			
			Context context = mock(Context.class);
			byte []valBytes = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
					0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
					0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
					0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38};
			ImmutableBytesWritable value = new ImmutableBytesWritable(valBytes);
			
			mapper.map(NullWritable.get(), value, context);
			
			byte []outBytes = {0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,
					0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
					0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,	
					0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28};
			
			ImmutableBytesWritable outKey = new ImmutableBytesWritable(outBytes);
			Put put = new Put(outBytes);
			put.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, null);
			
			verify(context).write(eq(outKey), argThat(new PutMatcher(put)));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void testOSPCMapper() {	
		try {
			PrefixMatch.PrefixMatchOSPCMapper mapper = new PrefixMatch.PrefixMatchOSPCMapper();
			
			Context context = mock(Context.class);
			byte []valBytes = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
					0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
					0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
					0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38};
			ImmutableBytesWritable value = new ImmutableBytesWritable(valBytes);
			
			mapper.map(NullWritable.get(), value, context);
			
			byte []outBytes = {0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
					0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
					0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
					0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38};
			
			ImmutableBytesWritable outKey = new ImmutableBytesWritable(outBytes);
			Put put = new Put(outBytes);
			put.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, null);
			
			verify(context).write(eq(outKey), argThat(new PutMatcher(put)));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
