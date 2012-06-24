package nl.vu.datalayer.hbase.bulkload;

import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import nl.vu.datalayer.hbase.id.HBaseValue;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;

public class StringIdAssocTest {
	
	@Test
	public void testId2String() {
		
		try {
			StringIdAssoc.Id2StringMapper mapper = new StringIdAssoc.Id2StringMapper();
			
			Context context = mock(Context.class);
			
			Value value = new LiteralImpl("Blabissimo");
			
			byte []keyBytes = {0x00, 0x01, 0x02, 0x03, 0x04, 0x5, 0x06, 0x07};
			ImmutableBytesWritable key = new ImmutableBytesWritable(keyBytes);
			HBaseValue hValue = new HBaseValue(value);
			mapper.map(key, hValue, context);
			
			ImmutableBytesWritable outKey = new ImmutableBytesWritable(keyBytes);
			Put outVal = new Put(keyBytes);
			byte []b = new byte[1];
			b[0] = (byte)HBaseValue.LITERAL_TYPE;
			outVal.add("CF".getBytes(), "".getBytes(), 
					Bytes.add(b, value.stringValue().getBytes("UTF-8")));
			
			verify(context).write(eq(outKey), argThat(new PrefixMatchTest.PutMatcher(outVal)));		
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testString2Id() {
		
		try {
			StringIdAssoc.String2IdMapper mapper = new StringIdAssoc.String2IdMapper();
			
			Context context = mock(Context.class);
			
			Value value = new LiteralImpl("Blabissimo");
			
			byte []keyBytes = {0x00, 0x01, 0x02, 0x03, 0x04, 0x5, 0x06, 0x07};
			ImmutableBytesWritable key = new ImmutableBytesWritable(keyBytes);
			HBaseValue hValue = new HBaseValue(value);
			
			mapper.setup(context);
			mapper.map(key, hValue, context);
			
			try {
				MessageDigest mDigest = MessageDigest.getInstance("MD5");
				byte []outBytes = mDigest.digest(value.toString().getBytes("UTF-8"));
				ImmutableBytesWritable outKey = new ImmutableBytesWritable(outBytes);
				
				Put outVal = new Put(outBytes);
				outVal.add("CF".getBytes(), "".getBytes(), keyBytes);
				
				verify(context).write(eq(outKey), argThat(new PrefixMatchTest.PutMatcher(outVal)));
			
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
