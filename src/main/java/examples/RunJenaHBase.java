package examples;

import java.io.IOException;

import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.HBaseFactory;
import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;
import nl.vu.jena.graph.HBaseGraph;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class RunJenaHBase {

	public static void main(String[] args) {

		HBaseConnection con;
		try {
			con = HBaseConnection.create(HBaseConnection.NATIVE_JAVA);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		HBaseClientSolution hbaseSol = HBaseFactory.getHBaseSolution(
				HBPrefixMatchSchema.SCHEMA_NAME, con, null);

		Graph g = new HBaseGraph(hbaseSol);
		Model model = ModelFactory.createModelForGraph(g);

		String queryString = "PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
				+ "SELECT ?name WHERE { "
				+ "    ?person foaf:mbox <mailto:alice@example.org> . "
				+ "    ?person foaf:name ?name . " + "}";
		ResultSet results = query(model, queryString);
		
		//System.out.println(m1.isIsomorphicWith(m2));
	}

	private static ResultSet query(Model model, String queryString) {
		Query query = QueryFactory.create(queryString);
		QueryExecution qexec = QueryExecutionFactory.create(query, model);
		//Model result = ModelFactory.createDefaultModel();
		ResultSet results;
		try {
			results = qexec.execSelect();
			//ResultSetFormatter.asRDF(result, results);
		} finally {
			qexec.close();
		}
		return results;
	}

}
