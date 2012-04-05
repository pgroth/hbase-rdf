package nl.vu.datalayer.hbase.sail;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import nl.vu.datalayer.hbase.NTripleParser;
import nl.vu.datalayer.hbase.RetrieveURI;

import info.aduna.iteration.CloseableIteration;

import org.openrdf.model.Literal;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.NotifyingSailConnectionBase;
import org.openrdf.sail.helpers.SailBase;

public class HBaseSailConnection extends NotifyingSailConnectionBase {

	public HBaseSailConnection(SailBase sailBase) {
		super(sailBase);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected void addStatementInternal(Resource arg0, URI arg1, Value arg2,
			Resource... arg3) throws SailException {
		if (arg3.length == 1) {
			try {
				FileWriter fstream = new FileWriter("data/buff.txt");
				BufferedWriter out = new BufferedWriter(fstream);
				
				out.write("<" + arg0.stringValue() + "> <" + arg1.stringValue() + "> ");
				if (arg2 instanceof Literal) {
					out.write("\"" + arg2.stringValue() + "\" .");
				}
				else {
					out.write("<" + arg2.stringValue() +">");
				}
				out.close();
				
				NTripleParser ntp = new NTripleParser("data/buff.txt", null);
				ntp.parse();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		else {
			SailException e = new SailException();
			throw e;
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
		// TODO Auto-generated method stub

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
		if (arg0 != null && arg4.length == 1) {
			RetrieveURI ruri = new RetrieveURI(arg4[0].stringValue());
			return ruri.retreieveSubject(arg0.stringValue());
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
