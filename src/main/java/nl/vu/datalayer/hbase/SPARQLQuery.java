package nl.vu.datalayer.hbase;

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

public class SPARQLQuery {

	/**
	 * @param args
	 * @throws SailException
	 * @throws RepositoryException
	 */
	public static void main(String[] args) throws SailException, RepositoryException {
		if (args.length < 2) {
//			System.out.println("USAGE: SPARQLQuery -f <query-file> [-s <memory-store-backup-file>]\n");
			System.out.println("\nUSAGE: SPARQLQuery -f <query-file>\n");
			System.out.println("-f <query-file>");
			System.out.println("    used to specify an input file for SPARQL queries");
			System.out.println("    use STD in place of the file name to provide a query in the command line");
			System.out.println("    use DEFAULT in place of the file name to use the default query file\n");
//			System.out.println("-s <memory-store-backup-file>");
//			System.out.println("    can be used to specify a backup file for the in-memory triple store");
			return;
		}
		
		String queryFile;
		if (args[1].equals("DEFAULT")) {
			queryFile = new String("data/test-queries.rq");
		}
		else if (args[1].equals("STD")) {
			queryFile = new String("STD");
		}
		else {
			queryFile = new String(args[1]);
		}
//		System.out.println(queryFile);
		
		HBaseSail mySail = new HBaseSail();
		mySail.initialize();
		HBaseSailRepository myRepo = new HBaseSailRepository(mySail);
		HBaseRepositoryConnection conn = myRepo.getConnection();
		
		try {
			BufferedReader br;
			if (queryFile.equals("STD")) {
				System.out.println("Write your SPARQL query followed by a newline, or CLOSE to quit");
				
				while (true) {
					BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
					String line = stdin.readLine();
					
					if (line.equals("CLOSE")) {
						return;
					} else {
						query(line, conn);
					}
				}
			} else {
				FileInputStream fstream = new FileInputStream(queryFile);
				DataInputStream in = new DataInputStream(fstream);
				br = new BufferedReader(new InputStreamReader(in));
				
				String queryString;
				while ((queryString = br.readLine()) != null) {
					query(queryString, conn);
				}
				in.close();
			}
		}
		catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}

//		conn.close();
	}
	
	private static void query(String queryString, HBaseRepositoryConnection conn) {
		System.out.println(queryString);
		
		String[] vars = {"g", "s", "p", "o"};
		
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
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
