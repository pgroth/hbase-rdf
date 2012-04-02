package nl.vu.datalayer.hbase;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.turtle.TurtleParser;


public class NTripleParser {
	
	public static void parse(String file, String schemaName) throws IOException {
		
		try {
			//HBaseUtil util = new HBaseUtil(confPath);
			FileInputStream is = new FileInputStream(file);
			RDFParser rdfParser = new TurtleParser();
			
			ArrayList<Statement> myList = new ArrayList<Statement>();
			StatementCollector collector = new StatementCollector(myList);
			rdfParser.setRDFHandler(collector);
			
			try {
			   rdfParser.parse(is, "");
			} 
			catch (IOException e) {
			  // handle IO problems (e.g. the file could not be read)
				e.printStackTrace();
			}
			catch (RDFParseException e) {
			  // handle unrecoverable parse error
				e.printStackTrace();
			}
			catch (RDFHandlerException e) {
			  // handle a problem encountered by the RDFHandler
				e.printStackTrace();
			}

			//connect to HBase
			HBaseConnection con = new HBaseConnection();
			HBaseClientSolution sol = HBaseFactory.getHBaseSolution(schemaName, con, myList);
			sol.schema.create();
			
			// populate table
			sol.util.populateTables(myList);
			
			con.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			// FileInputStream exception
		}
	}
	
	public static void main(String[] args) {
		try {
			//parse("tbl-card.nt", null);
			if (args.length != 2){
				System.out.println("Usage: NTripleParser <inputFile> <schemaName>");
				System.out.println("Schema names: predicate-cf, hexastore");
				return;
			}
			parse(args[0], args[1]);
		}
		catch (Exception e) {
		}
	}

}
