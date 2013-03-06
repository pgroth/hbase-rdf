package nl.vu.datalayer.hbase.retrieve;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import nl.vu.datalayer.hbase.id.Id;
import nl.vu.datalayer.hbase.operations.IHBaseOperationManager;

import org.openrdf.model.Value;

public interface IHBasePrefixMatchRetrieveOpsManager extends IHBaseOperationManager {
	
	public ArrayList<ArrayList<Value>> getMaterializedResults(HBaseTripleElement[] triple) throws IOException;
	
	public ArrayList<ArrayList<Value>> getMaterializedResults(HBaseTripleElement[] triple, RowLimitPair limits) throws IOException;
	
	//TODO switch to Id array as input
	public ArrayList<ArrayList<Id>> getResults(HBaseTripleElement[] triple) throws IOException;
	
	public ArrayList<ArrayList<Id>> getResults(HBaseTripleElement[] triple, RowLimitPair limits) throws IOException;
	
	public void materializeIds(Map<Id, Value> idMap) throws IOException;;

}
