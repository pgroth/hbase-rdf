package nl.vu.datalayer.hbase.id;


import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author sever
 *
 */
public class BaseId extends Id{
	
	public static final int SIZE = 8;
	
	/**
	 * @param partitionId
	 * @param localCounter
	 */
	public BaseId(int partitionId, long localCounter) {
		super();
		long tripleID = ((long)partitionId)<<24;
		tripleID |= localCounter & 0x0000000000ffffffL;
		id = Bytes.toBytes(tripleID);
	}
	
	public BaseId(byte[] id) {
		super(id);
	}
	
	public BaseId(long idValue){
		id = Bytes.toBytes(idValue);
	}
	
	public BaseId(){
		id = new byte[SIZE];
	}
	
	public void set(int partitionId, long localCounter){
		long tripleID = ((long)partitionId)<<24;
		tripleID |= localCounter & 0x0000000000ffffffL;
		Bytes.putLong(id, 0, tripleID);
	}

	public String toString(){
		String ret = "";
		for (int i = 0; i < id.length; i++) {
			ret += String.format("%02x ", id[i]);
		}
		return ret;
	}

	@Override
	public byte[] getContent() {
		return id;
	}

	@Override
	public int getContentOffset() {
		return 0;
	}
	
	
}
