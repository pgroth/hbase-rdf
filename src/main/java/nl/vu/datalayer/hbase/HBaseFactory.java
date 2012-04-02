package nl.vu.datalayer.hbase;

import java.util.ArrayList;

import nl.vu.datalayer.hbase.schema.HBHexastoreSchema;
import nl.vu.datalayer.hbase.schema.HBasePredicateCFSchema;
import nl.vu.datalayer.hbase.util.HBHexastoreUtil;
import nl.vu.datalayer.hbase.util.HBasePredicateCFUtil;

import org.openrdf.model.Statement;

public class HBaseFactory {
	
	public static HBaseClientSolution getHBaseSolution(String schemaName, HBaseConnection con, ArrayList<Statement> statements) throws Exception{
		if (schemaName.equals("predicate-cf")){
			HBasePredicateCFSchema schema = new HBasePredicateCFSchema(con, statements);
			return new HBaseClientSolution(schema,
										new HBasePredicateCFUtil(con, schema));
		}
		else{//default hexastore"
			HBHexastoreSchema schema = new HBHexastoreSchema(con);
			return new HBaseClientSolution(schema,
										new HBHexastoreUtil(con, schema));
		}
	}
}
