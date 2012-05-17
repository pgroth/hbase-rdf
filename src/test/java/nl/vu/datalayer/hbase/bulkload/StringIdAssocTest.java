package nl.vu.datalayer.hbase.bulkload;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;

import java.io.IOException;

import nl.vu.datalayer.hbase.bulkload.PrefixMatchTest.PutMatcher;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;

public class StringIdAssocTest {
	
	@Test
	public void test() {
		
		try {
			StringIdAssoc.Id2StringMapper mapper = new StringIdAssoc.Id2StringMapper();
			
			Context context = mock(Context.class);
			
			byte []keyBytes = {0x00, 0x01, 0x02, 0x03, 0x04, 0x5, 0x06, 0x07};
			ImmutableBytesWritable key = new ImmutableBytesWritable(keyBytes);
			
			Text value = new Text("Blabissimo");
			
			mapper.map(key, value, context);
			
			ImmutableBytesWritable outKey = new ImmutableBytesWritable(keyBytes);
			Put outVal = new Put(keyBytes);
			outVal.add("CF".getBytes(), "".getBytes(), value.getBytes());
			
			verify(context).write(eq(outKey), argThat(new PrefixMatchTest.PutMatcher(outVal)));		
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
