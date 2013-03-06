package nl.vu.datalayer.hbase;

import nl.vu.datalayer.hbase.operations.IHBaseOperations;
import nl.vu.datalayer.hbase.schema.IHBaseSchema;

/**
 * Encapsulates a schema and an associated util that can use that schema
 * @author Sever Fundatureanu
 *
 */
public class HBaseClientSolution {
	public IHBaseSchema schema;
	public IHBaseOperations util;
	
	public HBaseClientSolution(IHBaseSchema schema, IHBaseOperations util) {
		super();
		this.schema = schema;
		this.util = util;
	}
}
