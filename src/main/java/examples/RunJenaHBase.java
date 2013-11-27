package examples;

import java.io.IOException;
import java.util.Iterator;

import org.openjena.atlas.io.IndentedLineBuffer;
import org.openjena.atlas.io.IndentedWriter;

import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.HBaseFactory;
import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;
import nl.vu.jena.graph.HBaseGraph;
import nl.vu.jena.sparql.engine.main.HBaseStageGenerator;
import nl.vu.jena.sparql.engine.optimizer.HBaseOptimize;
import nl.vu.jena.sparql.engine.optimizer.HBaseTransformFilterPlacement;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Triple;
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
import com.hp.hpl.jena.sparql.ARQConstants;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.engine.QueryExecutionBase;
import com.hp.hpl.jena.sparql.engine.main.StageBuilder;
import com.hp.hpl.jena.sparql.serializer.SerializationContext;
import com.hp.hpl.jena.sparql.sse.WriterSSE;
import com.hp.hpl.jena.sparql.util.QueryOutputUtils;

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

		Graph g = new HBaseGraph(hbaseSol, HBaseGraph.CACHING_ON);
		Model model = ModelFactory.createModelForGraph(g);
		//FileManager.get().addLocatorClassLoader(RunJenaHBase.class.getClassLoader());
        //Model model = FileManager.get().loadModel("data/tbl-card2.ttl", null, "TURTLE");
        
        //printStatements(model);
		
		/*model.setNsPrefix("<http://purl.org/dc/elements/1.1>", "dc");
		//model.add(new ResourceImpl("<file:///home/tolgam/Documents/Divers/tbl-card.rdf>"), 
				 new PropertyImpl("dc:title"), 
				"Tim Berners-Lee's FOAF file");*/

		runSPARQLQuery(model);
	}

	public static void runSPARQLQuery(Model model) {
		String queryString = BSBMQueries.Q2;

		System.out.println("Query: \""+queryString+" \"");
		//Query query = QueryFactory.create(queryString);
		HBaseStageGenerator hbaseStageGenerator = new HBaseStageGenerator();
		StageBuilder.setGenerator(ARQ.getContext(), hbaseStageGenerator) ;
		
		ARQ.getContext().set(ARQConstants.sysOptimizerFactory, HBaseOptimize.hbaseOptimizationFactory);
		ARQ.getContext().set(ARQ.optFilterPlacement, new HBaseTransformFilterPlacement());
		
		QueryExecutionBase qexec = (QueryExecutionBase)QueryExecutionFactory.create(queryString, model);
		
		try {
			executeSelect(qexec);
			//executeDescribe(qexec, model);
		} finally {
			qexec.close();
		}
	}
	
	private static void executeDescribe(QueryExecutionBase qexec, Model model){
		Iterator<Triple> it = qexec.execDescribeTriples();
		while (it.hasNext()){
			System.out.println(it.next());
		}
	}

	private static void executeSelect(QueryExecutionBase qexec) {
		ResultSet results;
		results = qexec.execSelect();
		
		/*IndentedLineBuffer buff = new IndentedLineBuffer() ;
		Op op = Algebra.compile(qexec.getQuery()) ;
		op = Algebra.optimize(op) ;
		WriterSSE.out(buff, op, qexec.getQuery()) ;
		String str = buff.toString() ;
		
		System.out.println(str);*/
		//ResultSetFormatter.asRDF(result, results);
		System.out.println("Solutions: "+results.getRowNumber());
		while (results.hasNext()){
			QuerySolution solution = results.next();
			System.out.println(solution.toString());
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
