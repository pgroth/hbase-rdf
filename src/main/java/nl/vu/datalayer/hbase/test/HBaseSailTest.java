package nl.vu.datalayer.hbase.test;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import nl.vu.datalayer.hbase.sail.HBaseRepositoryConnection;
import nl.vu.datalayer.hbase.sail.HBaseSail;
import nl.vu.datalayer.hbase.sail.HBaseSailRepository;

import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

public class HBaseSailTest {

	/**
	 * @param args
	 * @throws SailException
	 * @throws RepositoryException
	 */
	public static void main(String[] args) throws SailException, RepositoryException {
		HBaseSail mySail = new HBaseSail();
		mySail.initialize();
		HBaseSailRepository myRepo = new HBaseSailRepository(mySail);
		HBaseRepositoryConnection conn = myRepo.getConnection();
		
		String[] vars = {"g", "s", "p", "o"};
		
		try {
			FileInputStream fstream = new FileInputStream("sparql_queries.txt");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			
			String queryString;
			while ((queryString = br.readLine()) != null) {
				System.out.println(queryString);
				
				try {
					TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
					TupleQueryResult result = tupleQuery.evaluate();
					while (result.hasNext()) {
						BindingSet bindingSet = result.next();
						
						for (int i = 0; i < vars.length; i++) {
							Value value = bindingSet.getValue(vars[i]);
							if (value != null) {
								System.out.println(vars[i] + " = " + value.stringValue() + "\n");
							}
						}
					}

				} catch (MalformedQueryException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (QueryEvaluationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			in.close();
		}
		catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}

//		conn.close();
	}

}
