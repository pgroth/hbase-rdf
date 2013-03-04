package nl.vu.datalayer.hbase.retrieve;

import java.io.IOException;
import java.util.ArrayList;

import nl.vu.datalayer.hbase.util.IHBaseUtil;

import org.openrdf.model.Value;

public interface IHBasePrefixMatchRetrieve extends IHBaseUtil {
	
	public ArrayList<ArrayList<Value>> getResults(Value[] triple, RowLimitPair limits) throws IOException;
	
	public ArrayList<ArrayList<Value>> getResults(HBaseGeneric[] triple) throws IOException;
	
	public ArrayList<ArrayList<Value>> getResults(HBaseGeneric[] triple, RowLimitPair limits) throws IOException;

}
