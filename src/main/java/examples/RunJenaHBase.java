package examples;

import java.io.IOException;

import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.HBaseFactory;
import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;
import nl.vu.jena.graph.HBaseGraph;
import nl.vu.jena.sparql.engine.main.HBaseStageGenerator;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.query.ARQ;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.engine.main.StageBuilder;

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
				"local-"+HBPrefixMatchSchema.SCHEMA_NAME, con, null);

		Graph g = new HBaseGraph(hbaseSol);
		Model model = ModelFactory.createModelForGraph(g);
		//FileManager.get().addLocatorClassLoader(RunJenaHBase.class.getClassLoader());
        //Model model = FileManager.get().loadModel("data/tbl-card2.ttl", null, "TURTLE");
        
        printStatements(model);
		
		/*model.setNsPrefix("<http://purl.org/dc/elements/1.1>", "dc");
		//model.add(new ResourceImpl("<file:///home/tolgam/Documents/Divers/tbl-card.rdf>"), 
				 new PropertyImpl("dc:title"), 
				"Tim Berners-Lee's FOAF file");*/

		runSPARQLQuery(model);
	}

	public static void runSPARQLQuery(Model model) {
		String queryString = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
			+"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
				+"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "

				+"SELECT DISTINCT ?product ?label "
				+"WHERE { "
				 +"?product rdfs:label ?label ."
				 +"?product a \"type1\" ."
				 +"?product bsbm:productFeature \"feature1\" ." 
				 +"?product bsbm:productFeature \"feature2\" ." 
				+"?product bsbm:productPropertyNumeric1 ?value1 ." 
					+"FILTER (?value1 > 20)} " 
				+"ORDER BY ?label "
				+"LIMIT 10";
/*"PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
				+ " SELECT * "
				+ " WHERE { "
				+ "    ?s ?p \"Tim Berners-Lee's FOAF file\" "
				+ "}";*/
		System.out.println("Query: \""+queryString+" \"");
		//Query query = QueryFactory.create(queryString);
		HBaseStageGenerator hbaseStageGenerator = new HBaseStageGenerator();
		StageBuilder.setGenerator(ARQ.getContext(), hbaseStageGenerator) ;
		
		QueryExecution qexec = QueryExecutionFactory.create(queryString, model);
		
		ResultSet results;
		try {
			results = qexec.execSelect();
			//ResultSetFormatter.asRDF(result, results);
			System.out.println("Solutions: "+results.getRowNumber());
			while (results.hasNext()){
				QuerySolution solution = results.next();
				System.out.println(solution.toString());
			}
		} finally {
			qexec.close();
		}
		
		
	}

	public static void printStatements(Model model) {
		StmtIterator iter = model.listStatements();
        try {
            while ( iter.hasNext() ) {
                Statement stmt = iter.next();
                
                Resource s = stmt.getSubject();
                Resource p = stmt.getPredicate();
                RDFNode o = stmt.getObject();
                
                System.out.print(s+" "+p+" "+o);
                
                /*if ( s.isURIResource() ) {
                    System.out.print("URI");
                } else if ( s.isAnon() ) {
                    System.out.print("blank");
                }
                
                if ( p.isURIResource() ) 
                    System.out.print(" URI ");
                
                if ( o.isURIResource() ) {
                    System.out.print("URI");
                } else if ( o.isAnon() ) {
                    System.out.print("blank");
                } else if ( o.isLiteral() ) {
                    System.out.print("literal");
                }*/
                
                System.out.println();                
            }
        } finally {
            if ( iter != null ) iter.close();
        }
	}

	private static ResultSet query(Model model, String queryString) {
		Query query = QueryFactory.create(queryString);
		QueryExecution qexec = QueryExecutionFactory.create(query, model);
		
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
