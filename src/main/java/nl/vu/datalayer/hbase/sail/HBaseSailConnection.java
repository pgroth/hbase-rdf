package nl.vu.datalayer.hbase.sail;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;

import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.HBaseConnection;
import nl.vu.datalayer.hbase.HBaseFactory;
import nl.vu.datalayer.hbase.NTripleParser;
import nl.vu.datalayer.hbase.RetrieveURI;
import nl.vu.datalayer.hbase.schema.HBHexastoreSchema;
import nl.vu.datalayer.hbase.schema.HBasePredicateCFSchema;
import nl.vu.datalayer.hbase.schema.IHBaseSchema;
import nl.vu.datalayer.hbase.util.HBasePredicateCFUtil;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.CloseableIteratorIteration;

import org.apache.commons.validator.UrlValidator;
import org.openrdf.model.Literal;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.turtle.TurtleParser;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.NotifyingSailConnectionBase;
import org.openrdf.sail.helpers.SailBase;

public class HBaseSailConnection extends NotifyingSailConnectionBase {

	HBaseConnection con;
	

	public HBaseSailConnection(SailBase sailBase) {
		super(sailBase);
		try {
			con = new HBaseConnection();
		} catch (Exception e) {
			e.printStackTrace();
			// TODO error handling
		}
	}

	@Override
	protected void addStatementInternal(Resource arg0, URI arg1, Value arg2,
			Resource... arg3) throws SailException {
		ArrayList<Statement> myList = new ArrayList();
		Statement s = new StatementImpl(arg0, arg1, arg2);
		myList.add(s);
		
		try {
			HBaseClientSolution sol = HBaseFactory.getHBaseSolution(HBHexastoreSchema.SCHEMA_NAME, con, myList);
			sol.schema.create();
			sol.util.populateTables(myList);
		}
		catch (Exception e) {
			e.printStackTrace();
			// TODO error handling
		}
	}

	@Override
	protected void clearInternal(Resource... arg0) throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void clearNamespacesInternal() throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void closeInternal() throws SailException {
		try {
			con.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			//TODO: error handling
		}
	}

	@Override
	protected void commitInternal() throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(
			TupleExpr arg0, Dataset arg1, BindingSet arg2, boolean arg3)
			throws SailException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected CloseableIteration<? extends Resource, SailException> getContextIDsInternal()
			throws SailException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String getNamespaceInternal(String arg0) throws SailException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected CloseableIteration<? extends Namespace, SailException> getNamespacesInternal()
			throws SailException {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	protected CloseableIteration<? extends Statement, SailException> getStatementsInternal(
			Resource arg0, URI arg1, Value arg2, boolean arg3, Resource... arg4)
			throws SailException {
		try {
			if (arg4.length == 0) {
				HBaseConnection con = new HBaseConnection();
				
				HBaseClientSolution sol = HBaseFactory.getHBaseSolution(HBHexastoreSchema.SCHEMA_NAME, con, null);
				
				String s = null;
				String p = null;
				String o = null;
				if (arg0 != null) {
					s = arg0.stringValue();
				}
				else {
					s = "?";
				}
				if (arg1 != null) {
					p = arg1.stringValue();
				}
				else {
					p = "?";
				}
				if (arg2 != null) {
					o = arg2.stringValue();
				}
				else {
					o = "?";
				}
				
				String []triple = {s, p, o};
				String triples = sol.util.getRawCellValue(triple[0], triple[1], triple[2]);
				
				InputStream is = new ByteArrayInputStream(triples.getBytes());
				RDFParser rdfParser = new TurtleParser();
				ArrayList<Statement> myList = new ArrayList<Statement>();
				StatementCollector collector = new StatementCollector(myList);
				rdfParser.setRDFHandler(collector);
				
				try {
				   rdfParser.parse(is, "");
				   Iterator it = myList.iterator();
				   CloseableIteration<Statement, SailException> ci = new CloseableIteratorIteration<Statement, SailException>(it);
				   return ci;
				}
				catch (Exception e) {
					Exception ex = new SailException("HBase output format error: " + e.getMessage());
					try {
						throw ex;
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
			else {
				Exception e = new SailException("Context information is not supported");
				throw e;
			}
		}
		catch (Exception e) {
			Exception ex = new SailException("HBase connection error: " + e.getMessage());
			try {
				throw ex;
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		return null;
	}

	@Override
	protected void removeNamespaceInternal(String arg0) throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void removeStatementsInternal(Resource arg0, URI arg1,
			Value arg2, Resource... arg3) throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void rollbackInternal() throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void setNamespaceInternal(String arg0, String arg1)
			throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected long sizeInternal(Resource... arg0) throws SailException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	protected void startTransactionInternal() throws SailException {
		// TODO Auto-generated method stub

	}

}
