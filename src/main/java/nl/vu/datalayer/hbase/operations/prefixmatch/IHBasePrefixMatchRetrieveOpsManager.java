package nl.vu.datalayer.hbase.operations.prefixmatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import nl.vu.datalayer.hbase.id.Id;
import nl.vu.datalayer.hbase.operations.IHBaseOperationManager;
import nl.vu.datalayer.hbase.parameters.RowLimitPair;

import org.openrdf.model.Value;

public interface IHBasePrefixMatchRetrieveOpsManager extends IHBaseOperationManager<Value> {
	
	public String []JOIN_POSITION={"S", "P", "O", "SP", "SO", "PO"};
	
	public void mapValuesToIds(Map<Value, Id> value2IdMap) throws IOException;
	
	public ArrayList<ArrayList<Id>> getResults(Id[] triple) throws IOException;
	
	public ArrayList<ArrayList<Id>> getResults(Id[] triple, RowLimitPair limits) throws IOException;
	
	public ArrayList<ArrayList<Id>> joinTriplePatterns(ArrayList<ArrayList<Id>> patterns, ArrayList<String> joinPositions) throws IOException;
	
	public void materializeIds(Map<Id, Value> id2ValueMap) throws IOException;

	public byte [] retrieveId(Value val) throws IOException;

}
