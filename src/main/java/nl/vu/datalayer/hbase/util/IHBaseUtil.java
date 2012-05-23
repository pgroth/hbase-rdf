package nl.vu.datalayer.hbase.util;

import java.io.IOException;
import java.util.ArrayList;

import org.openrdf.model.Statement;

public interface IHBaseUtil {
	/**
	 * Retrieve parsed triple/quad
	 * @param triple/quad
	 * @return
	 * @throws IOException
	 */
	public ArrayList<ArrayList<String>> getRow(String []triple) throws IOException;
	
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
