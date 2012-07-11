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
		try{
			/*Text t = new Text("abcdefg");
			String s = Text.decode(t.getBytes(), 0, t.getLength());
			System.out.println(t.toString());
			System.out.println(t.getLength()+" "+t.getBytes().length+";");
			
			
			long hash = hashFunction(s);
			System.out.println(hash);*/
			
			byte b = (byte)0xa3;
			//byte b2 = (byte)((short)b/2);
			//String byteStr = ""+(byte)literal.charAt(literal.length()-1);
			//System.out.println(byteStr);
			//byte b = -1;
			
			//System.out.println(String.b2);
			System.out.println(String.format("%04x", (byte)(b & 0xfe)));
			//System.out.println(String.format("%02x", (byte)(literal.charAt(literal.length()-1))/2));
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
