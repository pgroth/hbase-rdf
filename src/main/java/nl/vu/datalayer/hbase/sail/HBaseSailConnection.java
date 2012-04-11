package nl.vu.datalayer.hbase.sail;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailQuery;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.turtle.TurtleParser;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.NotifyingSailConnectionBase;
import org.openrdf.sail.helpers.SailBase;
import org.openrdf.sail.memory.MemoryStore;
import org.openrdf.sail.memory.MemoryStoreConnection;

import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.query.parser.ParsedTupleQuery;


public class HBaseSailConnection extends NotifyingSailConnectionBase {

	HBaseConnection con;
	
    //Builder to write the query to bit by bit
    StringBuilder queryString = new StringBuilder();
	

	public HBaseSailConnection(SailBase sailBase) {
		super(sailBase);
		System.out.println("SailConnection created");
		con = ((HBaseSail)sailBase).getHBaseConnection();
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
	}

	@Override
	protected void commitInternal() throws SailException {
		// TODO Auto-generated method stub

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
	
	
	protected CloseableIteration<? extends Statement, SailException> getStatementsInternal(
			Resource arg0, URI arg1, Value arg2, boolean arg3, Resource... arg4)
			throws SailException {
		try {
			if (arg4 == null) {
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

	/**
	 * This functions returns a List of all RDF statements in HBase
	 * that are needed in the query.
	 * 
	 * @param arg0
	 * @return
	 * @throws SailException
	 */
	protected ArrayList<Statement> evaluateInternal(TupleExpr arg0) throws SailException {
		ArrayList<Statement> result = new ArrayList();
		
		try {
			ArrayList<ArrayList<Var>> statements = HBaseQueryVisitor.convertToStatements(arg0, null, null);
			System.out.println("StatementPatterns: " + statements.size());
			
			Iterator it = statements.iterator();
			while (it.hasNext()) {
				ArrayList<Var> sp = (ArrayList<Var>)it.next();
				
				Resource subj = null;
				URI pred = null;
				Value obj = null;
				Iterator jt = sp.iterator();
				int index = 0;
				
				while (jt.hasNext()) {
					Var var = (Var)jt.next();
					
					if (index == 0) {
						if (var.hasValue()) {
				            subj = new URIImpl(var.getValue().stringValue());
				        } else if (var.isAnonymous()) {
				        	subj = new BNodeImpl(var.getName()); 
				        	
				        }
					}
					else if (index == 1) {
						if (var.hasValue()) {
				            pred = new URIImpl(var.getValue().stringValue());
				        }
						
					}
					else {
						if (var.hasValue()) {
				            obj = new LiteralImpl(var.getValue().stringValue());
				        } else if (var.isAnonymous()) {
				        	obj = new BNodeImpl(var.getName());
				        }
					}
					index += 1;
				}
				
				CloseableIteration ci = getStatementsInternal(subj, pred, obj, false, null);
				
				while (ci.hasNext()) {
					Statement statement = (Statement)ci.next();
					result.add(statement);
				}
			}
		}
		catch (Exception e) {
			throw new SailException(e);
		}
		
		return result;
	}
	
	/**
	 * This function retrieves all the triples from HBase that
	 * match with StatementPatterns in the SPARQL query, without
	 * executing the SPARQL query on them.
	 */
	@Override
	protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(
			TupleExpr arg0, Dataset arg1, BindingSet arg2, boolean arg3)
			throws SailException {
		ArrayList<MapBindingSet> result = new ArrayList();
		Set<String> bindingSet = arg2.getBindingNames();
		
		try {
			ArrayList<ArrayList<Var>> statements = HBaseQueryVisitor.convertToStatements(arg0, null, null);
			
			Iterator it = statements.iterator();
			while (it.hasNext()) {
				ArrayList<Var> sp = (ArrayList<Var>)it.next();

				String[] variables = {"", "", ""};
				MapBindingSet mapBindingSet  = new MapBindingSet();
				
				Resource subj = null;
				URI pred = null;
				Value obj = null;
				Iterator jt = sp.iterator();
				int index = 0;
				
				while (jt.hasNext()) {
					Var var = (Var)jt.next();
					
					if (index == 0) {
						if (var.hasValue()) {
				            subj = new URIImpl(var.getValue().stringValue());
				        } else if (var.isAnonymous()) {
				        	subj = new BNodeImpl(var.getName()); 
				        	
				        } else {
				        	variables[index] = var.getName();
				        }
					}
					else if (index == 1) {
						if (var.hasValue()) {
				            pred = new URIImpl(var.getValue().stringValue());
				        } else {
				        	variables[index] = var.getName();
				        }
						
					}
					else {
						if (var.hasValue()) {
				            obj = new LiteralImpl(var.getValue().stringValue());
				        } else if (var.isAnonymous()) {
				        	obj = new BNodeImpl(var.getName()); 
				        	
				        } else {
				        	variables[index] = var.getName();
				        }
					}
					index += 1;
				}
				
				CloseableIteration ci = getStatementsInternal(subj, pred, obj, false, null);
				
				while (ci.hasNext()) {
					Statement statement = (Statement)ci.next();
					Value[] values = {statement.getSubject(), statement.getPredicate(), statement.getObject()};
					
					for (int i = 0; i < 3; i ++) {
						if (variables[i] != "" && bindingSet.contains(variables[i])) {
							mapBindingSet.addBinding(variables[i], values[i]);
						}
					}
				}
				result.add(mapBindingSet);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new SailException(e);
		}
		
		Iterator it = result.iterator();
		CloseableIteration<MapBindingSet, QueryEvaluationException> ci = new CloseableIteratorIteration<MapBindingSet, QueryEvaluationException>(it);
		
		return ci;
	}
	
	
	/**
	 * This function retrieves the relevant triples from HBase,
	 * loads them into an in-memory store, then evaluates the SPARQL query on them.
	 * 
	 * @param tupleExpr 
	 * @param dataset
	 * @param bindings
	 * @param includeInferred
	 * @return
	 * @throws SailException
	 */
	public CloseableIteration<? extends BindingSet,QueryEvaluationException> query(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {
		System.out.println("Evaluating query");
		try {
			ArrayList<Statement> statements = evaluateInternal(tupleExpr);
			System.out.println("Statements retrieved: " + statements.size());

			MemoryStore memStore = new MemoryStore();
			memStore.initialize();
			NotifyingSailConnection con = memStore.getConnection();
			System.out.println("Created memory store");
			Iterator it = statements.iterator();
			while (it.hasNext()) {
				Statement statement = (Statement)it.next();
				Resource[] context = {new URIImpl("http://hbase.sail.vu.nl")};
				con.addStatement(statement.getSubject(), statement.getPredicate(), statement.getObject(), context);
			}
			
			CloseableIteration<? extends BindingSet, QueryEvaluationException> ci = con.evaluate(tupleExpr, dataset, bindings, includeInferred);
			con.close();
			
			return ci;
			
		} catch (SailException e) {
			e.printStackTrace();
			throw e;
		}
	}

}
