package nl.vu.datalayer.hbase.operations;

import java.io.IOException;
import java.util.ArrayList;

import nl.vu.datalayer.hbase.retrieve.RowLimitPair;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;

public interface IHBaseOperations {//TODO Look into using templates
	/**
	 * Retrieve parsed triple/quad
	 * 
	 * @param triple
	 *            /quad
	 * @return
	 * @throws IOException
	 */
	public ArrayList<ArrayList<String>> getResults(String[] triple) throws IOException;

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
	 * Populate the database with the statements
	 * 
	 * @param statements
	 * @throws Exception
	 */
	public void populateTables(ArrayList<Statement> statements) throws Exception;

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
