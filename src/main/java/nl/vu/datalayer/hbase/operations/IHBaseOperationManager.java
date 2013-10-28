package nl.vu.datalayer.hbase.operations;

import java.io.IOException;
import java.util.ArrayList;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;

public interface IHBaseOperationManager<T> {

	/**
	 * Solves a simple query in which un-bound variable are expected to be null
	 * 
	 * @param quad
	 *            - array of T objects with null elements in un-bound
	 *            positions
	 * @return list of results null - in case of errors
	 * @throws IOException
	 */
	public ArrayList<ArrayList<T>> getResults(T[] quad) throws IOException;

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
	public long countResults(T[] quad, long hardLimit) throws IOException;
}
