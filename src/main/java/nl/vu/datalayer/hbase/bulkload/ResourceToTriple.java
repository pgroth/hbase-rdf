package nl.vu.datalayer.hbase.bulkload;

import java.io.IOException;
import java.util.Iterator;

import nl.vu.datalayer.hbase.id.BaseId;
import nl.vu.datalayer.hbase.id.DataPair;
import nl.vu.datalayer.hbase.id.TypedId;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ResourceToTriple {
	
	public static final String TEMP_TRIPLETS_DIR = "/tempTriplets";
	
	public static class ResourceToTripleMapper extends MapReduceBase implements Mapper<TypedId, DataPair, BaseId, DataPair>
	{
		@Override
		public void map(TypedId key, DataPair dataPair,
				 OutputCollector<BaseId, DataPair> output, Reporter r) throws IOException{			
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
			
			output.collect(tripleId, dataPair);
		}	
	}
	
	public static class ResourceToTripleReducer extends MapReduceBase implements Reducer<BaseId, DataPair, NullWritable, ImmutableBytesWritable>
	{
		public static int[] offsets = {0, 8, 16, 25};
		public static int outSize = BaseId.SIZE*3+TypedId.SIZE;
		
		@Override
		public void reduce(BaseId id, Iterator<DataPair> it,  OutputCollector<NullWritable, ImmutableBytesWritable> output, Reporter r)
							throws IOException{
			//System.err.println("Reducer: "+id);
			
			byte []outValue = new byte[outSize];
			
			int counter = 0;
			while (it.hasNext()) {
				DataPair pair = it.next();
				Bytes.putBytes(outValue, offsets[pair.getPosition()], 
						pair.getId().getBytes(), 0, pair.getId().getBytes().length);
				counter++;
			}
			
			if (counter != 4){
				System.err.println("Unexpected number of elements in each quad: "+counter);
				return;
			}
			
			output.collect(NullWritable.get(), new ImmutableBytesWritable(outValue));
		}
	}

}
