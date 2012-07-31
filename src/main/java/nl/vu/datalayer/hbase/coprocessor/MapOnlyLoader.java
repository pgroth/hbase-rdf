package nl.vu.datalayer.hbase.coprocessor;

import java.io.IOException;

import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;

public class MapOnlyLoader {
	
	public static class Mapper extends 
			org.apache.hadoop.mapreduce.Mapper<NullWritable, ImmutableBytesWritable, 
								NullWritable, NullWritable>
	{
		private HTable spocTable;
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			spocTable.close();
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			String suffix = context.getConfiguration().get("SUFFIX");
			spocTable = new HTable(HBaseConfiguration.create(), HBPrefixMatchSchema.TABLE_NAMES[HBPrefixMatchSchema.SPOC]+suffix);
			spocTable.setAutoFlush(false);
		}

		@Override
		protected void map(NullWritable key, ImmutableBytesWritable value, Context context) throws IOException, InterruptedException {
			Put outValue = new Put(value.get());
			outValue.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, null);
			
			spocTable.put(outValue);
		}
		
	}

}
