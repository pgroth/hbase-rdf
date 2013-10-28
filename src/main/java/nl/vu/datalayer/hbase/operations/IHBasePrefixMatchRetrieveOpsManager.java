package nl.vu.datalayer.hbase.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import nl.vu.datalayer.hbase.id.Id;
import nl.vu.datalayer.hbase.parameters.RowLimitPair;

import org.openrdf.model.Value;

public interface IHBasePrefixMatchRetrieveOpsManager extends IHBaseOperationManager<Value> {
	
	public void mapValuesToIds(Map<Value, Id> value2IdMap) throws IOException;
	
	public ArrayList<ArrayList<Id>> getResults(Id[] triple) throws IOException;
	
	public ArrayList<ArrayList<Id>> getResults(Id[] triple, RowLimitPair limits) throws IOException;
	
	public void materializeIds(Map<Id, Value> id2ValueMap) throws IOException;

	public byte [] retrieveId(Value val) throws IOException;

}
