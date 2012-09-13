package nl.vu.datalayer.hbase.sail;

import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.SailException;

/**
 * A repository that operates on a stack of {@link HBaseSail} objects.
 * <p>
 * To create and intialize the repository, you can use the following code:
 * <pre>
 * {@code
 * HBaseSail sail = new HBaseSail();
 * sail.initialize();
 * 
 * HBaseSailRepository myRepo = new HBaseSailRepository(sail);
 * }
 * </pre>
 * 
 * @author Anca Dumitrache, Antonis Loizou
 */
public class HBaseSailRepository extends SailRepository {
	

	/**
	 * Creates a new repository object that operates on the supplied HBaseSail.
	 * 
	 * @param sail An HBaseSail object.
	 */
	public HBaseSailRepository(HBaseSail sail) {
		super(sail);
	}

	/**
	 * Opens a connection to this repository that can be used for querying and updating the
	 * contents of the repository. The connection does not need to be closed
	 * 
	 * @return A connection that allows operations on this repository.
	 */
	@Override
	public HBaseRepositoryConnection getConnection() throws RepositoryException {
		try {
			return new HBaseRepositoryConnection(this);
		} catch (SailException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	protected HBaseSail getHBaseSail() {
		return (HBaseSail) getSail();
	}

}
