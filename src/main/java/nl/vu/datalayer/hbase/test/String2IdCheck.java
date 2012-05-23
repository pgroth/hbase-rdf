package nl.vu.datalayer.hbase.test;

import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.HBaseConnection;
import nl.vu.datalayer.hbase.HBaseFactory;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;
import nl.vu.datalayer.hbase.util.HBPrefixMatchUtil;

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
			HBaseConnection con = new HBaseConnection();
		
			HBaseClientSolution sol = HBaseFactory.getHBaseSolution(HBPrefixMatchSchema.SCHEMA_NAME, con, null);
			HBPrefixMatchUtil util = (HBPrefixMatchUtil)sol.util;
			
			byte []id = util.retrieveId(args[0]);
			if (id != null)
				System.out.println("Id: "+HBPrefixMatchUtil.hexaString(id));
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
