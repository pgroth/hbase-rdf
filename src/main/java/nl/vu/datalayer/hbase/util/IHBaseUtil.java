package nl.vu.datalayer.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;

public interface IHBaseUtil {
	/**
	 * Retrieve parsed triple/quad
	 * 
	 * @param triple
	 *            /quad
	 * @return
	 * @throws IOException
	 */
	public ArrayList<ArrayList<String>> getRow(String[] triple) throws IOException;

	/**
	 * Solves a simple query in which un-bound variable are expected to be null
	 * 
	 * @param quad
	 *            - array of Value objects with null elements in un-bound
	 *            positions
	 * @return list of results null - in case of errors
	 * @throws IOException
	 */
	public ArrayList<ArrayList<Value>> getResults(Value[] quad) throws IOException;

	/**
	 * Triple with "?" elements in the unbound positions
	 * 
	 * @param triple
	 * @return
	 * @throws IOException
	 */
	public String getRawCellValue(String subject, String predicate, String object) throws IOException;

	/**
	 * Populate the database with the statements
	 * 
	 * @param statements
	 * @throws Exception
	 */
	public void populateTables(ArrayList<Statement> statements) throws Exception;

	/**
	 * Test if a given quad with un-bound positions matches other quads in the
	 * data set
	 * 
	 * @param quad
	 *            - array of Value objects with null elements in un-bound
	 *            positions
	 * @return true if there is at least one quad that matches, false otherwise
	 */
	public boolean hasResults(Value[] quad) throws IOException;

	/**
	 * Return a single matching quad picked at random
	 * 
	 * @param quad
	 *            the pattern (un-bound positions are null)
	 * @param random
	 *            the random number generator to use
	 * @return
	 * @throws IOException
	 */
	public ArrayList<Value> getSingleResult(Value[] quad, Random random) throws IOException;

	/**
	 * Return the number of quads matching the quad pattern
	 * 
	 * @param quad
	 * @param hardLimit
	 *            maximum number of quads to be counted for, used to stop
	 *            counting after a while
	 * @return
	 * @throws IOException
	 */
	public long countResults(Value[] quad, long hardLimit) throws IOException;
}
