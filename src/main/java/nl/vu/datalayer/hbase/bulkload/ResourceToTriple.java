package nl.vu.datalayer.hbase.bulkload;

import java.io.IOException;

import nl.vu.datalayer.hbase.exceptions.QuadSizeException;
import nl.vu.datalayer.hbase.id.BaseId;
import nl.vu.datalayer.hbase.id.DataPair;
import nl.vu.datalayer.hbase.id.Id;
import nl.vu.datalayer.hbase.id.TypedId;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;

public class ResourceToTriple {
	
	public static final String TEMP_TRIPLETS_DIR = "/tempTriplets";
	
	public static long getMapOutputRecordSizeEstimate() { 
		return TypedId.SIZE+Bytes.SIZEOF_BYTE+//id + length
				BaseId.SIZE+Bytes.SIZEOF_BYTE+//id + length
				Bytes.SIZEOF_BYTE;//position in Data Pair
	}
	
	public static class ResourceToTripleMapper extends org.apache.hadoop.mapreduce.Mapper<TypedId, DataPair, BaseId, DataPair>
	{
		private Id baseId = new BaseId();
		
		@Override
		public void map(TypedId key, DataPair dataPair, Context context) throws IOException, InterruptedException {
			BaseId tripleId = (BaseId)dataPair.getId();
			
			Id resourceId;
			if (dataPair.getPosition() == DataPair.O){
				//we keep typed Ids only in Object positions
				resourceId = key;
			}
			else{//non-Object positions can only be URIs or Blank nodes
				baseId.set(key.getContent());
				resourceId = baseId;
			}
			dataPair.setId(resourceId);
			
			context.write(tripleId, dataPair);
		}
	}
	
	public static class ResourceToTripleReducer extends org.apache.hadoop.mapreduce.Reducer<BaseId, DataPair, NullWritable, ImmutableBytesWritable>
	{
		public static int[] offsets = {0, 8, 16, 25};
		public static int outSize = BaseId.SIZE*3+TypedId.SIZE;
		protected byte [] outValues = new byte[outSize];
		private ImmutableBytesWritable immutableBytesWritable =  new ImmutableBytesWritable();		
		
		@Override
		public void reduce(BaseId tripleId, Iterable<DataPair> values, Context context) throws IOException, InterruptedException {
			try {
				buildKey(values);
			} catch (QuadSizeException e) {
				System.err.println(e.getMessage());
				return;
			}
			
			immutableBytesWritable.set(outValues);
			context.write(NullWritable.get(), immutableBytesWritable);
		}

		protected void buildKey(Iterable<DataPair> values) throws QuadSizeException{	
			int counter = 0;
			for (DataPair pair : values){
				byte []pairIdBytes = pair.getId().getBytes(); 
				Bytes.putBytes(outValues, offsets[pair.getPosition()], 
						pairIdBytes, 0, pairIdBytes.length);
				counter++;
			}
			
			if (counter != 4){
				throw new QuadSizeException("Unexpected number of elements in each quad: "+counter);
			}
		}
	}

}
