package nl.vu.datalayer.hbase.test;


public class TextTest {
	
	private static long hashFunction(String key){
		long hash = 0;
		
		for (int i = 0; i < key.length(); i++) {
			hash = (int)key.charAt(i) + (hash << 6) + (hash << 16) - hash;
		}
		
		return hash;
	}
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		byte b = (byte)0x85;
		
		System.out.println(Integer.toHexString(~b&0x80));
		
		System.out.println(Integer.toHexString(b&0x7f));
		
		
		b = (byte)((~b&0x80) | (b&0x7f));
		
		System.out.println(Integer.toHexString(((~b&0x80) | (b&0x7f))));
	}

}
