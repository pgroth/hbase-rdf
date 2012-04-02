package nl.vu.datalayer.hbase.schema;


public interface IHBaseSchema {
	
	/**
	 * Creates the tables associated with this schema
	 * @throws Exception
	 */
	public void create() throws Exception;
}
