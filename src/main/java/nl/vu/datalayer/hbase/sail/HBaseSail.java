package nl.vu.datalayer.hbase.sail;

import java.io.IOException;
import java.util.ArrayList;

import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.HBaseConnection;
import nl.vu.datalayer.hbase.HBaseFactory;

import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.NotifyingSailBase;

public class HBaseSail extends NotifyingSailBase {

	private HBaseClientSolution hbase;
	private HBaseConnection con;
	
	private ValueFactory valueFactory = new ValueFactoryImpl();
	
	public HBaseSail() {
		try {
			HBaseConnection con = new HBaseConnection();
			hbase = HBaseFactory.getHBaseSolution("hexastore", con, new ArrayList<Statement>());
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	@Override
	public ValueFactory getValueFactory() {
		return valueFactory;
	}
	
	HBaseClientSolution getHBase() {
		return hbase;
	}

	HBaseConnection getHBaseConnection() {
		return con;
	}

	@Override
	public boolean isWritable() throws SailException {
		return true;
	}

	@Override
	protected NotifyingSailConnection getConnectionInternal()
			throws SailException {
		return new HBaseSailConnection(this);
	}

	@Override
	protected void shutDownInternal() throws SailException {
		try {
			con.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
