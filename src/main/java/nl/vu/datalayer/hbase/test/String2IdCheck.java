package nl.vu.datalayer.hbase.test;

import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.HBaseFactory;
import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.operations.HBPrefixMatchOperationManager;
import nl.vu.datalayer.hbase.operations.IHBasePrefixMatchRetrieveOpsManager;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

public class String2IdCheck {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 1){
			System.out.println("Usage: String2IdCheck <string> <type>");
			return;
		}
		
		try{
			HBaseConnection con = HBaseConnection.create(HBaseConnection.NATIVE_JAVA);
		
			HBaseClientSolution sol = HBaseFactory.getHBaseSolution(HBPrefixMatchSchema.SCHEMA_NAME, con, null);
			IHBasePrefixMatchRetrieveOpsManager util = (IHBasePrefixMatchRetrieveOpsManager)sol.opsManager;
			
			ValueFactory valFactory = new ValueFactoryImpl();
			Value val = null;
			if (args[1].equals("URI")){
				val = valFactory.createURI(args[0]);
			}
			
			byte []id = util.retrieveId(val);
			if (id != null)
				System.out.println("Id: "+HBPrefixMatchOperationManager.hexaString(id));
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
