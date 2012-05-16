package nl.vu.datalayer.hbase.test;

import nl.vu.datalayer.hbase.id.BaseId;

import org.apache.hadoop.hbase.util.Bytes;

public class IdTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		BaseId t = new BaseId(0x65241297, 0x432343134522L);
		
		long l = Bytes.toLong(t.getBytes());
		System.out.println("Triple ID: "+String.format("%016x", l));
	}

}
