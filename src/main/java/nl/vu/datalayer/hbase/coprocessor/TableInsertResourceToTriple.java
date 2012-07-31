package nl.vu.datalayer.hbase.coprocessor;

import java.io.IOException;

import nl.vu.datalayer.hbase.bulkload.ResourceToTriple;
import nl.vu.datalayer.hbase.exceptions.QuadSizeException;
import nl.vu.datalayer.hbase.id.BaseId;
import nl.vu.datalayer.hbase.id.DataPair;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class TableInsertResourceToTriple extends ResourceToTriple.ResourceToTripleReducer {

	private HTable spocTable = null;
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		spocTable.close();
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		String suffix = context.getConfiguration().get("SUFFIX");
		spocTable = new HTable(HBaseConfiguration.create(), HBPrefixMatchSchema.TABLE_NAMES[HBPrefixMatchSchema.SPOC]+suffix);
		spocTable.setAutoFlush(false);
		spocTable.setWriteBufferSize(12*1024*1024);
	}

	@Override
	public void reduce(BaseId tripleId, Iterable<DataPair> values, Context context) throws IOException, InterruptedException {
		try {
			buildKey(values);
		} catch (QuadSizeException e) {
			System.err.println(e.getMessage());
			return;
		}
		
		Put put = new Put(outValues);
		put.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, null);
		
		spocTable.put(put);
	}	
}
