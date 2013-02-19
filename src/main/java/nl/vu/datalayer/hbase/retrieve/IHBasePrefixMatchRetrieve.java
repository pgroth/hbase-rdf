package nl.vu.datalayer.hbase.retrieve;

import java.io.IOException;
import java.util.ArrayList;

import org.openrdf.model.Value;

import nl.vu.datalayer.hbase.util.IHBaseUtil;

public interface IHBasePrefixMatchRetrieve extends IHBaseUtil {
	
	public ArrayList<ArrayList<Value>> getResults(Value[] triple, RowLimitPair limits) throws IOException;

}
