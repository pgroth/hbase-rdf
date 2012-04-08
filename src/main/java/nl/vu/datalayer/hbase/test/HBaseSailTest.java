package nl.vu.datalayer.hbase.test;

import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import nl.vu.datalayer.hbase.sail.HBaseRepositoryConnection;
import nl.vu.datalayer.hbase.sail.HBaseSail;
import nl.vu.datalayer.hbase.sail.HBaseSailRepository;

public class HBaseSailTest {

	/**
	 * @param args
	 * @throws SailException 
	 * @throws RepositoryException 
	 */
	public static void main(String[] args) throws SailException, RepositoryException {
		// TODO Auto-generated method stub
		HBaseSail mySail = new HBaseSail();
		mySail.initialize();
		HBaseSailRepository myRepo = new HBaseSailRepository(mySail);
		HBaseRepositoryConnection conn = myRepo.getConnection();
		
	}

}
