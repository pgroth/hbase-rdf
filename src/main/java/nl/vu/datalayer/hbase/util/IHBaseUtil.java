package nl.vu.datalayer.hbase.util;

import java.io.IOException;
import java.util.ArrayList;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;

public interface IHBaseUtil {
	/**
	 * Retrieve parsed triple/quad
	 * @param triple/quad
	 * @return
	 * @throws IOException
	 */
	public ArrayList<ArrayList<String>> getRow(String []triple) throws IOException;
	
	/**
	 * Solves a simple query in which un-bound variable are expected to be null
	 * 
	 * @param quad
	 * @return
	 * @throws IOException
	 */
	public ArrayList<ArrayList<Value>> getResults(Value []quad) throws IOException;
	
	/**
	 * Triple with "?" elements in the unbound positions
	 * @param triple
	 * @return
	 * @throws IOException
	 */
	public String getRawCellValue(String subject, String predicate, String object) throws IOException;
	
	/**
	 * Populate the database with the statements
	 * @param statements
	 * @throws Exception
	 */
	public void populateTables(ArrayList<Statement> statements) throws Exception;
}
