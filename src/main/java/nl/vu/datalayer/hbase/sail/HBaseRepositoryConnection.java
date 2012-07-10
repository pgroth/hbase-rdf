package nl.vu.datalayer.hbase.sail;

import nl.vu.datalayer.hbase.NTripleParser;
import nl.vu.datalayer.hbase.RetrieveURI;


import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.Iteration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.Dataset;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.TupleQueryResultBuilder;
import org.openrdf.query.impl.TupleQueryResultImpl;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailBooleanQuery;
import org.openrdf.repository.sail.SailGraphQuery;
import org.openrdf.repository.sail.SailQuery;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.repository.sail.SailTupleQuery;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

public class HBaseRepositoryConnection extends SailRepositoryConnection {
	
	HBaseSailConnection conn;

	protected HBaseRepositoryConnection(HBaseSailRepository repository) throws SailException {
		super(repository, repository.getHBaseSail().getConnection());
		conn = new HBaseSailConnection(repository.getHBaseSail());
	}
	
	private HBaseSailConnection getHBaseSailConnection() {
		return (HBaseSailConnection) getSailConnection();
	}
	
	@Override
	public SailConnection getSailConnection() {
		return conn;
	}

	@Override
	public TupleQuery prepareTupleQuery(QueryLanguage lang, String query)
			throws RepositoryException, MalformedQueryException {
		return prepareTupleQuery(lang, query, "http://hbase.sail.vu.nl");
	}

	@Override
	public SailTupleQuery prepareTupleQuery(QueryLanguage lang, String query, String baseURI) 
			throws MalformedQueryException {
		ParsedTupleQuery parsedQuery = QueryParserUtil.parseTupleQuery(lang, query, baseURI);
		
//		Dataset dataset = parsedQuery.getDataset();
//		System.out.println("DATASET: " + dataset.toString());
		
		return new HBaseSailTupleQuery(query, parsedQuery, this);
	}

	@Override
	public GraphQuery prepareGraphQuery(QueryLanguage lang, String query)
			throws RepositoryException, MalformedQueryException {
		return prepareGraphQuery(lang, query, "http://hbase.sail.vu.nl");
	}

	@Override
	public SailGraphQuery prepareGraphQuery(QueryLanguage lang, String query, String baseURI) 
			throws MalformedQueryException {
		return null;//new HBaseSailGraphQuery(parsedQuery, this);
	}

	@Override
	public BooleanQuery prepareBooleanQuery(QueryLanguage arg0, String arg1)
			throws RepositoryException, MalformedQueryException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SailBooleanQuery prepareBooleanQuery(QueryLanguage arg0, String arg1,
			String arg2) throws MalformedQueryException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Query prepareQuery(QueryLanguage arg0, String arg1)
			throws RepositoryException, MalformedQueryException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SailQuery prepareQuery(QueryLanguage arg0, String arg1, String arg2)
			throws MalformedQueryException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void add(Statement stmt, Resource... contexts)
			throws RepositoryException {
		try {
			getSailConnection().addStatement(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), contexts);
		} catch (SailException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void add(File file, String baseURI, RDFFormat format, Resource... contexts)
			throws IOException, RDFParseException, RepositoryException {
		if (format == RDFFormat.NTRIPLES) {
//			getHBaseSailConnection().add(file, baseURI, format, contexts);
			
		}
		else {
			RDFParseException e = new RDFParseException("Unsupported RDF Format");
			throw e;
		}
		
	}

	@Override
	public void add(Resource s, URI p, Value o, Resource... contexts)
			throws RepositoryException {
		try {
			getSailConnection().addStatement(s, p, o, contexts);
		} catch (SailException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void clear(Resource... arg0) throws RepositoryException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void clearNamespaces() throws RepositoryException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws RepositoryException {
		try {
			this.getSailConnection().close();
		} catch (SailException e) {
			throw new RepositoryException(e);
		}
		
	}

	@Override
	public void commit() throws RepositoryException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void export(RDFHandler arg0, Resource... arg1)
			throws RepositoryException, RDFHandlerException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void exportStatements(Resource arg0, URI arg1, Value arg2,
			boolean arg3, RDFHandler arg4, Resource... arg5)
			throws RepositoryException, RDFHandlerException {
		if (arg0 != null && arg5.length == 1) {
			RetrieveURI.retrieveURI(arg5[0].stringValue(), new BufferedWriter(new OutputStreamWriter(System.out)));
			RetrieveURI.printURIInfo(arg0.stringValue());
		}
		
	}

	@Override
	public RepositoryResult<Resource> getContextIDs()
			throws RepositoryException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getNamespace(String arg0) throws RepositoryException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RepositoryResult<Namespace> getNamespaces()
			throws RepositoryException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RepositoryResult<Statement> getStatements(Resource arg0, URI arg1,
			Value arg2, boolean arg3, Resource... arg4)
			throws RepositoryException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ValueFactory getValueFactory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean hasStatement(Statement arg0, boolean arg1, Resource... arg2)
			throws RepositoryException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean hasStatement(Resource arg0, URI arg1, Value arg2,
			boolean arg3, Resource... arg4) throws RepositoryException {
		/*if (arg0 != null && arg4.length == 1) {
			RetrieveURI.retrieveURI(arg4[0].stringValue(),new BufferedWriter(new OutputStreamWriter(System.out)));
			ArrayList<Statement> list = ruri.retrieveSubject(arg0.stringValue());
			
			Iterator it = list.iterator();
			while (it.hasNext()) {
				Statement st = (Statement)it.next();
				if (st.getPredicate().stringValue() == arg1.stringValue() && st.getObject().stringValue() == arg2.stringValue()) {
					return true;
				}
			}
			return false;
		}*/
		return false;
	}

	@Override
	public boolean isAutoCommit() throws RepositoryException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isEmpty() throws RepositoryException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isOpen() throws RepositoryException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void remove(Statement arg0, Resource... arg1)
			throws RepositoryException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void remove(Iterable<? extends Statement> arg0, Resource... arg1)
			throws RepositoryException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <E extends Exception> void remove(
			Iteration<? extends Statement, E> arg0, Resource... arg1)
			throws RepositoryException, E {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void remove(Resource arg0, URI arg1, Value arg2, Resource... arg3)
			throws RepositoryException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeNamespace(String arg0) throws RepositoryException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rollback() throws RepositoryException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setAutoCommit(boolean arg0) throws RepositoryException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setNamespace(String arg0, String arg1)
			throws RepositoryException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long size(Resource... arg0) throws RepositoryException {
		// TODO Auto-generated method stub
		return 0;
	}

	private static class HBaseSailTupleQuery extends SailTupleQuery {
		
		String queryString;
		Dataset context;

		protected HBaseSailTupleQuery(String qs, final ParsedTupleQuery parsedTupleQuery, final HBaseRepositoryConnection HBaseRepositoryConnection) {
			super(parsedTupleQuery, HBaseRepositoryConnection);
			queryString = qs;
			context = parsedTupleQuery.getDataset();
		}

		@Override
		public TupleQueryResult evaluate() throws QueryEvaluationException {
			TupleQueryResultBuilder aBuilder = new TupleQueryResultBuilder();

			evaluate(aBuilder);

			return aBuilder.getQueryResult();
		}

		@Override
		public void evaluate(TupleQueryResultHandler handler) throws QueryEvaluationException {
			try {
//				Set<String> bindingSet = getBindings().getBindingNames();
				List<String> bindingList = new ArrayList<String>();
				
				HBaseSailConnection connection = ((HBaseRepositoryConnection)this.getConnection()).getHBaseSailConnection();
				TupleExpr te = getParsedQuery().getTupleExpr();
				Dataset dataset = getDataset();
				
//				System.out.println("ORIGINAL TUPLE EXPRESSION: " + te.toString());
//				System.out.println("CONTEXT: " + context.toString());
				
				TupleQueryResult result = connection.query(te, context, getBindings(), getIncludeInferred());
				
//				System.out.println("TupleQueryResult bindings: " + result.getBindingNames().size());
				
//				Get all triples from HBase, without evaluating them against the
//				memory triple store.
//				
//				TupleQueryResult result = new TupleQueryResultImpl(bindingList, 
//						((HBaseRepositoryConnection)this.getConnection()).getHBaseSailConnection().evaluateInternal(getParsedQuery().getTupleExpr(), getDataset(), getBindings(), getIncludeInferred()));

				System.out.println("WE MADE IT");
				
				int ressize = 0;
				handler.startQueryResult(result.getBindingNames());
				while (result.hasNext()) {
					BindingSet binding = result.next();
//					System.out.println("x = " + binding.getValue("x").stringValue());
					handler.handleSolution(binding);
					ressize += 1;
				}
				
				handler.endQueryResult();
			}
			catch (Exception e) {
				throw new QueryEvaluationException(e);
			}
		}
	}
	
}
