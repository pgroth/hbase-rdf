package nl.vu.datalayer.sail;

import java.io.File;

import org.openrdf.model.ValueFactory;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;

public class HBaseRepository extends SailRepository {
	
	String configuration;
	

	public HBaseRepository(Sail sail, String c) {
		super(sail);
		configuration = c;
	}

	@Override
	public HBaseRepositoryConnection getConnection() throws RepositoryException {
		HBaseConnection conn = new HBaseConnection();
		HBaseRepositoryConnection repoConn = new HBaseRepositoryConnection(this, conn); 
		return repoConn;
	}

	@Override
	public ValueFactory getValueFactory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void initialize() throws RepositoryException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void shutDown() throws RepositoryException {
		// TODO Auto-generated method stub
		
	}

}
