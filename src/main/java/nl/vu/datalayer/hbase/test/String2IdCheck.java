package nl.vu.datalayer.hbase.test;

import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.HBaseFactory;
import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.operations.HBPrefixMatchOperationManager;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

public class String2IdCheck {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 1){
			System.out.println("Usage: String2IdCheck <string>");
			return;
		}
		
		try{
			HBaseConnection con = HBaseConnection.create(HBaseConnection.NATIVE_JAVA);
		
			HBaseClientSolution sol = HBaseFactory.getHBaseSolution(HBPrefixMatchSchema.SCHEMA_NAME, con, null);
			HBPrefixMatchOperationManager util = (HBPrefixMatchOperationManager)sol.opsManager;
			
			byte []id = util.retrieveId(args[0]);
			if (id != null)
				System.out.println("Id: "+HBPrefixMatchOperationManager.hexaString(id));
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
