package nl.vu.datalayer.hbase;

import nl.vu.datalayer.hbase.operations.IHBaseOperationManager;
import nl.vu.datalayer.hbase.schema.IHBaseSchema;

/**
 * Encapsulates a schema and an associated util that can use that schema
 * @author Sever Fundatureanu
 *
 */
public class HBaseClientSolution {
	public IHBaseSchema schema;
	public IHBaseOperationManager opsManager;
	
	public HBaseClientSolution(IHBaseSchema schema, IHBaseOperationManager opsManager) {
		super();
		this.schema = schema;
		this.opsManager = opsManager;
	}
}
