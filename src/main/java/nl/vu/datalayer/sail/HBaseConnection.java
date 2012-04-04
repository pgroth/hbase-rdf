package nl.vu.datalayer.sail;

import info.aduna.iteration.CloseableIteration;

import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UpdateExpr;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

public class HBaseConnection implements SailConnection {

	@Override
	public void addStatement(Resource arg0, URI arg1, Value arg2,
			Resource... arg3) throws SailException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void clear(Resource... arg0) throws SailException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void clearNamespaces() throws SailException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws SailException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit() throws SailException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(
			TupleExpr arg0, Dataset arg1, BindingSet arg2, boolean arg3)
			throws SailException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CloseableIteration<? extends Resource, SailException> getContextIDs()
			throws SailException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getNamespace(String arg0) throws SailException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CloseableIteration<? extends Namespace, SailException> getNamespaces()
			throws SailException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CloseableIteration<? extends Statement, SailException> getStatements(
			Resource arg0, URI arg1, Value arg2, boolean arg3, Resource... arg4)
			throws SailException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isOpen() throws SailException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void removeNamespace(String arg0) throws SailException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeStatements(Resource arg0, URI arg1, Value arg2,
			Resource... arg3) throws SailException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rollback() throws SailException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setNamespace(String arg0, String arg1) throws SailException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long size(Resource... arg0) throws SailException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void executeUpdate(UpdateExpr arg0, Dataset arg1, BindingSet arg2,
			boolean arg3) throws SailException {
		// TODO Auto-generated method stub
		
	}

}
