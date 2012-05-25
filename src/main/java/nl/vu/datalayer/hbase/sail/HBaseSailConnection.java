package nl.vu.datalayer.hbase.sail;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.CloseableIteratorIteration;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.HBaseFactory;
import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.schema.HBHexastoreSchema;

import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.query.impl.TupleQueryResultImpl;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.NotifyingSailConnectionBase;
import org.openrdf.sail.helpers.SailBase;
import org.openrdf.sail.memory.MemoryStore;


public class HBaseSailConnection extends NotifyingSailConnectionBase {

	MemoryStore memStore;
	NotifyingSailConnection memStoreCon;
	HBaseClientSolution hbase;
	
    //Builder to write the query to bit by bit
    StringBuilder queryString = new StringBuilder();
	

	public HBaseSailConnection(SailBase sailBase) {
		super(sailBase);
//		System.out.println("SailConnection created");
		hbase = ((HBaseSail)sailBase).getHBase();

		memStore = new MemoryStore();
		try {
			memStore.initialize();
			memStoreCon = memStore.getConnection();
		} catch (SailException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	protected void addStatementInternal(Resource arg0, URI arg1, Value arg2,
			Resource... arg3) throws SailException {
		ArrayList<Statement> myList = new ArrayList();
		Statement s = new StatementImpl(arg0, arg1, arg2);
		myList.add(s);
		
		// TODO: update method for adding quads
		
//		try {
//			HBaseClientSolution sol = HBaseFactory.getHBaseSolution(HBHexastoreSchema.SCHEMA_NAME, con, myList);
//			sol.schema.create();
//			sol.util.populateTables(myList);
//		}
//		catch (Exception e) {
//			e.printStackTrace();
//			// TODO error handling
//		}
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
		memStoreCon.close();
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
			String s = null;
			String p = null;
			String o = null;
			ArrayList<String> g = new ArrayList();
			
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

			if (arg4 != null) {
				for (Resource r : arg4) {
					g.add(r.stringValue());
				}
			}
			else {
				g.add("?");
			}
			
			ArrayList<Statement> myList = new ArrayList();
			for (String graph : g) {
				String []triple = {s, p, o, graph};
				ArrayList<ArrayList<String>> triples = hbase.util.getRow(triple);
				
				for (ArrayList<String> tr : triples) {
					for (String st : tr) {
						System.out.print(st + " ");
					}
					System.out.print("\n");
				}
				
	//			System.out.println("Raw triples: " + triples);
				
				myList.addAll(reconstructTriples(triples, triple));
			}
				
//			System.out.println("Triples retrieved:");
//			System.out.println(myList.toString());
				
			Iterator it = myList.iterator();
			CloseableIteration<Statement, SailException> ci = new CloseableIteratorIteration<Statement, SailException>(it);
			return ci;
	
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
	
	protected ArrayList<Statement> reconstructTriples(ArrayList<ArrayList<String>> result, String[] triple) throws SailException {
		ArrayList<Statement> list = new ArrayList();
		
		for (ArrayList<String> arrayList : result) {
			int index = 0;
			
			Resource s = null;
			URI p = null;
			Value o = null;
			Resource c = null;
			
			for (String value : arrayList) {
				if (index == 0) {
					s = (Resource)getSubject(value);
				}
				else if (index == 1) {
					p = (URI) getPredicate(value);
				} else if (index == 2) {

					o = getObject(value);
				} else {
					if (value.compareTo("?") == 0) {
						Statement statement = new StatementImpl(s, p, o);
						list.add(statement);
					} else {
						c = (Resource)getContext(value);
						Statement statement = new ContextStatementImpl(s, p, o, c);
						list.add(statement);
					}
				}
				index++;
			}
		}
		return list;
	}
	
	Value getSubject(String s) {
		System.out.println("SUBJECT: " + s);
		if (s.startsWith("_")) {
			return new BNodeImpl(s.substring(2));
		}
		return new URIImpl(s);
	}
	
	Value getPredicate(String s) {
		System.out.println("PREDICATE: " + s);
		return new URIImpl(s);
	}
	
	Value getObject(String s) {
		System.out.println("OBJECT: " + s);
		if (s.startsWith("_")) {
			return new BNodeImpl(s.substring(2));
		}
		else if (s.startsWith("\"")) {
			String literal = "";
			String language = "";
			String datatype = "";
			
			for (int i = 1; i < s.length(); i++) {
				while (s.charAt(i) != '"') {
					
					// read literal value
					literal += s.charAt(i);
					if (s.charAt(i) == '\\') {
						i++;
						literal += s.charAt(i);
					}
					i++;
					if (i == s.length()) {
						// EOF exception
					}
				}
				System.out.println(literal);
				
				// charAt(i) = '"', read next char
				i++;
				
				if (s.charAt(i) == '@') {
					// read language
					i++;
					while (i < s.length()) {
						language += s.charAt(i);
					}
					System.out.println(language);
					return new LiteralImpl(literal, language);
				}
				else if (s.charAt(i) == '^') {
					// read datatype
					i++;
					
					// check for second '^'
					if (i == s.length()) {
						// EOF exception
					}
					else if (s.charAt(i) != '^') {
						// incorrect formatting exception
					}
					i++;
					
					// check for '<'
					if (i == s.length()) {
						// EOF exception
					}
					else if (s.charAt(i) != '<') {
						// incorrect formatting exception
					}
					i++;
					
					while (s.charAt(i) != '>') {
						datatype += s.charAt(i); 
						i++;
						if (i == s.length()) {
							// EOF exception
						}
					}
					System.out.println(datatype);
					return new LiteralImpl(literal, new URIImpl(datatype));
				}
				else {
					return new LiteralImpl(literal);
				}
				
			}
		}
		return new URIImpl(s);
	}
	
	Value getContext(String s) {
		System.out.println("GRAPH: " + s);
		return new URIImpl(s);
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
//			System.out.println("StatementPatterns: " + statements.size());
			
			Iterator it = statements.iterator();
			while (it.hasNext()) {
				ArrayList<Var> sp = (ArrayList<Var>)it.next();
				ArrayList<Var> contexts = HBaseQueryVisitor.getContexts(arg0);
				ArrayList<Resource> cons = new ArrayList();
				
//				System.out.println("CONTEXTS:");
//				if (contexts != null) {
//					for (Var con : contexts) {
//						System.out.println("CONTEXT: " + con.toString());
//	//					cons.add((Resource)new URIImpl(con.));
//					}
//				}
				
				Resource subj = null;
				URI pred = null;
				Value obj = null;
				Iterator jt = sp.iterator();
				
				
				int index = 0;
				
				while (jt.hasNext()) {
					Var var = (Var)jt.next();
					
					if (index == 0) {
						if (var.hasValue()) {
				            subj = (Resource)getSubject(var.getValue().stringValue());
				        } else if (var.isAnonymous()) {
				        	subj = (Resource)getSubject(var.getName()); 
				        	
				        }
					}
					else if (index == 1) {
						if (var.hasValue()) {
				            pred = (URI)getPredicate(var.getValue().stringValue());
				        }
						
					}
					else {
						if (var.hasValue()) {
				            obj = (Value)getObject(var.getValue().stringValue());
				        } else if (var.isAnonymous()) {
				        	obj = (Value)getObject(var.getName());
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
			ArrayList<Var> contexts = HBaseQueryVisitor.getContexts(arg0);
			
			for (Var con : contexts) {
				System.out.println("CONTEXT: " + con.toString());
			}
			
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
				            subj = (Resource)getSubject(var.getValue().stringValue());
				        } else if (var.isAnonymous()) {
				        	subj = (Resource)getSubject(var.getName()); 
				        	
				        }
					}
					else if (index == 1) {
						if (var.hasValue()) {
				            pred = (URI)getPredicate(var.getValue().stringValue());
				        }
						
					}
					else {
						if (var.hasValue()) {
				            obj = (Value)getObject(var.getValue().stringValue());
				        } else if (var.isAnonymous()) {
				        	obj = (Value)getObject(var.getName());
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
	public TupleQueryResult query(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {
//		System.out.println("Evaluating query");
		try {
			ArrayList<Statement> statements = evaluateInternal(tupleExpr);
//			System.out.println("Statements retrieved: " + statements.size());
			
			Iterator it = statements.iterator();
			while (it.hasNext()) {
				Statement statement = (Statement)it.next();
				Resource[] context = {new URIImpl("http://hbase.sail.vu.nl")};
				memStoreCon.addStatement(statement.getSubject(), statement.getPredicate(), statement.getObject(), context);
			}
			
			CloseableIteration<? extends BindingSet, QueryEvaluationException> ci = memStoreCon.evaluate(tupleExpr, dataset, bindings, includeInferred);
			CloseableIteration<? extends BindingSet, QueryEvaluationException> cj = memStoreCon.evaluate(tupleExpr, dataset, bindings, includeInferred);
			
			List<String> bindingList = new ArrayList<String>();
			int index = 0;
			while (ci.hasNext()) {
				index++;
				BindingSet bs = (BindingSet)ci.next();
//                System.out.println("Binding size(" + index + "): " + bs.getBindingNames().size());
				Set<String> localBindings = bs.getBindingNames();
				Iterator jt = localBindings.iterator();
				while (jt.hasNext()) {
					String binding = (String)jt.next();
					if (bindingList.contains(binding) == false) {
						bindingList.add(binding);
//						System.out.println("Added binding: " + binding);
					}
				}
			}
//			System.out.println("Results retrieved from memory store: " + index);
//			System.out.println("Bindings retrieved from memory store: " + bindingList.size());
			
			
			TupleQueryResult result = new TupleQueryResultImpl(bindingList, cj);
			
//			int ressize = 0;
//			while (result.hasNext()) {
//				BindingSet binding = result.next();
//				System.out.println("x = " + binding.getValue("x").stringValue());
//				ressize += 1;
//			}
//			System.out.println("TupleQueryResult size: " + ressize);
			
			return result;
			
		} catch (SailException e) {
			e.printStackTrace();
			throw e;
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			throw new SailException(e);
		}
	}

}
