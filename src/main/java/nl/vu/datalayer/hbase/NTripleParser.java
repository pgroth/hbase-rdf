package nl.vu.datalayer.hbase;



import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.io.FilenameUtils;

import org.openrdf.model.Resource;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.turtle.TurtleParser;

import java.io.IOException;


public class NTripleParser {
	
	public static void parse(String file, String confPath) throws IOException {
		
		try {
			HBaseUtil util = new HBaseUtil(confPath);
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
			}
			catch (RDFParseException e) {
			  // handle unrecoverable parse error
			}
			catch (RDFHandlerException e) {
			  // handle a problem encountered by the RDFHandler
			}
			
			String tableName = FilenameUtils.removeExtension(FilenameUtils.getName(file));
			
			// create table column families
			ArrayList<String> predicates = new ArrayList<String>();
			for (Iterator<Statement> iter = myList.iterator(); iter.hasNext();) {
				Statement s = iter.next();
				if (s.getObject() instanceof Resource) {
					predicates.add(s.getPredicate().stringValue());
				}
				else {
					// predicates.add("literal:" + s.getPredicate().stringValue());
				}
			}
			util.createTableStruct(tableName, predicates);
			util.cachePredicates();
			
			// populate table
			for (Iterator<Statement> iter = myList.iterator(); iter.hasNext();) {
				Statement s = iter.next();
				if (s.getObject() instanceof Resource){
					util.addRow(tableName, s.getSubject().toString(), s.getPredicate().toString(), "", s.getObject().toString());
				}
				else {
					util.addRow(tableName, s.getSubject().toString(), "literal", s.getPredicate().toString(), s.getObject().toString());
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			// FileInputStream exception
		}
	}
	
	public static void main(String[] args) {
		try {
			parse("tbl-card.nt", null);
			//parse(args[0]);
		}
		catch (Exception e) {
		}
	}

}
