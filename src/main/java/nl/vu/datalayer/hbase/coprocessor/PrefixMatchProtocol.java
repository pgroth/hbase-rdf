package nl.vu.datalayer.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface PrefixMatchProtocol extends CoprocessorProtocol {
	
	public void generateSecondaryIndex() throws IOException;
	
}
