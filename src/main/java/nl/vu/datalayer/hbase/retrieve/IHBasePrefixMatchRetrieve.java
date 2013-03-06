package nl.vu.datalayer.hbase.retrieve;

import java.io.IOException;
import java.util.ArrayList;

import nl.vu.datalayer.hbase.id.Id;
import nl.vu.datalayer.hbase.operations.IHBaseOperations;

import org.openrdf.model.Value;

public interface IHBasePrefixMatchRetrieve extends IHBaseOperations {
	
	public ArrayList<ArrayList<Value>> getMaterializedResults(HBaseGeneric[] triple) throws IOException;
	
	public ArrayList<ArrayList<Value>> getMaterializedResults(HBaseGeneric[] triple, RowLimitPair limits) throws IOException;
	
	public ArrayList<ArrayList<Id>> getResults(HBaseGeneric[] triple) throws IOException;
	
	public ArrayList<ArrayList<Id>> getResults(HBaseGeneric[] triple, RowLimitPair limits) throws IOException;

}
