package nl.vu.datalayer.hbase.test;

public class HBPrefixMatchSchemaTest {

	/**
	 * @param args
	 */
	/*
	 * public static void main(String[] args) {
	 * 
	 * byte []startKey = new byte[33]; byte []endKey = new byte[33]; String
	 * startKeyString = ""; String endKeyString = ""; for (int i = 0; i <
	 * endKey.length; i++) { endKey[i] = (byte)0xff; //startKeyString +=
	 * String.format("%02x", startKey[i]); //endKeyString +=
	 * String.format("%02x", endKey[i]); }
	 * 
	 * long startPartition = 0; int numInputPartitions = 111; long
	 * startKeyPrefix = startPartition << 24; Bytes.putLong(startKey, 0,
	 * startKeyPrefix); long endKeyPrefix =
	 * ((startPartition+numInputPartitions-1) << 24) | 0xffffffL;
	 * Bytes.putLong(endKey, 0, endKeyPrefix);
	 * System.out.println("Endkeysize: "+
	 * endKey.length+"; Start key size: "+startKey.length);
	 * System.out.println("Start: "+String.format("%x",
	 * startKeyPrefix)+"; End: "+String.format("%x", endKeyPrefix));
	 * 
	 * for (int i = 0; i < startKey.length; i++) {
	 * System.out.print(String.format("%02x", startKey[i])); }
	 * System.out.println(); for (int i = 0; i < endKey.length; i++) {
	 * System.out.print(String.format("%02x", endKey[i])); }
	 * System.out.println();
	 * 
	 * byte [][]splits = HBPrefixMatchSchema.getSplits(startKey, endKey,
	 * HBPrefixMatchSchema.NUM_REGIONS);
	 * 
	 * 
	 * for (int i = 0; i < splits.length; i++) { for (int j = 0; j <
	 * splits[i].length; j++) { System.out.print(String.format("%02x",
	 * splits[i][j])); } System.out.println(); }
	 * 
	 * }
	 */

	/*
	 * public static void main(String[] args) {
	 * 
	 * SortedMap<Byte, Long> prefixCounters = new TreeMap();
	 * 
	 * //prefixCounters.put("file://", 2L); //prefixCounters.put("http://",
	 * 8L);//100 mil http //prefixCounters.put("_:", 1L);
	 * prefixCounters.put((byte)0x20, 2100L); prefixCounters.put((byte)0x21,
	 * 2000L);
	 * 
	 * long totalCount = 4100;
	 * 
	 * //prefixCounters.put("", totalCount-21597799L);
	 * 
	 * HBPrefixMatchSchema.setString2IdTableSplitInfo(totalCount,
	 * prefixCounters);
	 * 
	 * byte [][]splits = HBPrefixMatchSchema.getString2IdSplits(64);
	 * 
	 * for (int i = 0; i < splits.length; i++) { for (int j = 0; j <
	 * splits[i].length; j++) { System.out.print(String.format("%02x",
	 * splits[i][j])); } System.out.println(); } }
	 */

	/*
	 * public static void main(String[] args) {
	 * 
	 * long startPartition = 0; int numInputPartitions = 111; long
	 * totalNumberOfString = 193426919L; long numericalParam = 22694894L;
	 * 
	 * HBPrefixMatchSchema.setId2StringTableSplitInfo(numInputPartitions,
	 * startPartition);
	 * HBPrefixMatchSchema.setObjectPrefixTableSplitInfo(totalNumberOfString,
	 * numericalParam);
	 * 
	 * byte [][]splits = HBPrefixMatchSchema.getObjectPrefixSplits(64);
	 * 
	 * for (int i = 0; i < splits.length; i++) { for (int j = 0; j <
	 * splits[i].length; j++) { System.out.print(String.format("%02x",
	 * splits[i][j])); } System.out.println(); } }
	 */

}
