package nl.vu.datalayer.hbase.test;

import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import nl.vu.datalayer.hbase.sail.HBaseRepositoryConnection;
import nl.vu.datalayer.hbase.sail.HBaseSail;
import nl.vu.datalayer.hbase.sail.HBaseSailRepository;

public class HBaseSailTest {

	/**
	 * @param args
	 * @throws SailException 
	 * @throws RepositoryException 
	 */
	public static void main(String[] args) throws SailException, RepositoryException {
		// TODO Auto-generated method stub
		HBaseSail mySail = new HBaseSail();
		mySail.initialize();
		HBaseSailRepository myRepo = new HBaseSailRepository(mySail);
		HBaseRepositoryConnection conn = myRepo.getConnection();
		
		String queryString = "SELECT ?g WHERE { GRAPH ?g { <http://dbpedia.org/resource/Alabama> <http://dbpedia.org/ontology/abstract> ?o  } }";
		System.out.println(queryString);
		
		try {
			TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		    TupleQueryResult result = tupleQuery.evaluate();
		    while (result.hasNext()) {
		    	BindingSet bindingSet = result.next();

//		    	Value valueOfP = bindingSet.getValue("p");
//		    	System.out.println("?p = " + valueOfP.stringValue());

		    	Value valueOfO = bindingSet.getValue("g");
		    	System.out.println("?g = " + valueOfO.stringValue() + "\n");
		    }
		    
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		conn.close();
	}

}
