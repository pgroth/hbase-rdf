package nl.vu.datalayer.hbase.sail;

import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;

public class HBaseSailRepository extends SailRepository {
	

	public HBaseSailRepository(HBaseSail sail) {
		super(sail);
	}

	@Override
	public HBaseRepositoryConnection getConnection() throws RepositoryException {
		try {
			return new HBaseRepositoryConnection(this);
		} catch (SailException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	protected HBaseSail getHBaseSail() {
		return (HBaseSail) getSail();
	}

}
