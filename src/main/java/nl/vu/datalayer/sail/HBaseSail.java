package nl.vu.datalayer.sail;

import nl.vu.datalayer.hbase.HBaseUtil;

import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.NotifyingSailBase;

public class HBaseSail extends NotifyingSailBase {

	private HBaseUtil hbase;
	
	private ValueFactory valueFactory = new ValueFactoryImpl();
	
	public HBaseSail() {
		try {
			hbase=new HBaseUtil(null);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	@Override
	public ValueFactory getValueFactory() {
		return valueFactory;
	}
	
	HBaseUtil getHBase() {
		return hbase;
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
		
	}

}
