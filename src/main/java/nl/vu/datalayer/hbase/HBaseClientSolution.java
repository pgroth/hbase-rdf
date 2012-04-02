package nl.vu.datalayer.hbase;

import nl.vu.datalayer.hbase.schema.IHBaseSchema;
import nl.vu.datalayer.hbase.util.IHBaseUtil;

/**
 * Encapsulates a schema and an associated util that can use that schema
 * @author Sever Fundatureanu
 *
 */
public class HBaseClientSolution {
	public IHBaseSchema schema;
	public IHBaseUtil util;
	
	public HBaseClientSolution(IHBaseSchema schema, IHBaseUtil util) {
		super();
		this.schema = schema;
		this.util = util;
	}
}
