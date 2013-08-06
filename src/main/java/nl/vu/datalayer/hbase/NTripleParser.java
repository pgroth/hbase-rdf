package nl.vu.datalayer.hbase;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;

import net.fortytwo.sesametools.nquads.NQuadsParser;
import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.connection.NativeJavaConnection;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.StatementCollector;


public class NTripleParser {
	
	public static void parse(String directoryPath, String schemaName) throws IOException {
		
		try {
			//HBaseUtil util = new HBaseUtil(confPath);
			//connect to HBase
			NativeJavaConnection con = (NativeJavaConnection)HBaseConnection.create(HBaseConnection.NATIVE_JAVA);
			HBaseClientSolution sol = HBaseFactory.getHBaseSolution(schemaName, con, null);
			sol.schema.create();
			((HBPrefixMatchSchema)sol.schema).createCounterTable(con.getAdmin());
			HBPrefixMatchSchema.updateCounter(0, 0, ((HBPrefixMatchSchema)sol.schema).getSchemaSuffix());
			HBPrefixMatchSchema.updateLastCounter(1, con.getConfiguration(), ((HBPrefixMatchSchema)sol.schema).getSchemaSuffix());
			
			File directory = new File(directoryPath);
			for (File child : directory.listFiles()) {
				FileInputStream is = new FileInputStream(child);
				RDFParser rdfParser = new NQuadsParser();
				
				ArrayList<Statement> myList = new ArrayList<Statement>();
				StatementCollector collector = new StatementCollector(myList);
				rdfParser.setRDFHandler(collector);
				
				System.out.println("Started parsing ..");
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
				System.out.println("Finished parsing: "+myList.size());
				// populate table
				sol.opsManager.populateTables(myList);
			}		
			
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
				System.out.println("Schema names: predicate-cf, hexastore, prefix-match");
				return;
			}
			parse(args[0], args[1]);
		}
		catch (Exception e) {
		}
	}

}
