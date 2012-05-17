package nl.vu.datalayer.hbase.bulkload;

import java.io.IOException;

import nl.vu.datalayer.hbase.id.BaseId;
import nl.vu.datalayer.hbase.id.DataPair;
import nl.vu.datalayer.hbase.id.TypedId;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;

public class ResourceToTriple {
	
	public static final String TEMP_TRIPLETS_DIR = "/tempTriplets";
	
	public static class ResourceToTripleMapper extends org.apache.hadoop.mapreduce.Mapper<TypedId, DataPair, BaseId, DataPair>
	{
		@Override
		public void map(TypedId key, DataPair dataPair, Context context) throws IOException, InterruptedException {
			BaseId tripleId = (BaseId)dataPair.getId();
			
			BaseId resourceId;
			if (dataPair.getPosition() == DataPair.O){
				//we keep typed Ids only in Object positions
				resourceId = key;
			}
			else{//non-Object positions can only be URIs or Blank nodes
				resourceId = new BaseId(key.getContent());
			}
			dataPair.setId(resourceId);
			
			context.write(tripleId, dataPair);
		}
	}
	
	public static class ResourceToTripleReducer extends org.apache.hadoop.mapreduce.Reducer<BaseId, DataPair, NullWritable, ImmutableBytesWritable>
	{
		public static int[] offsets = {0, 8, 16, 25};
		public static int outSize = BaseId.SIZE*3+TypedId.SIZE;
		
		@Override
		public void reduce(BaseId tripleId, Iterable<DataPair> values, Context context) throws IOException, InterruptedException {
			byte []outValue = new byte[outSize];
			
			int counter = 0;
			for (DataPair pair : values){
				Bytes.putBytes(outValue, offsets[pair.getPosition()], 
						pair.getId().getBytes(), 0, pair.getId().getBytes().length);
				counter++;
			}
			
			if (counter != 4){
				System.err.println("Unexpected number of elements in each quad: "+counter);
				return;
			}
			
			context.write(NullWritable.get(), new ImmutableBytesWritable(outValue));
		}
	}

}
