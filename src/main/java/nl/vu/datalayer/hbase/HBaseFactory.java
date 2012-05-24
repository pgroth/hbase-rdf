package nl.vu.datalayer.hbase;

import java.util.ArrayList;

import nl.vu.datalayer.hbase.schema.HBHexastoreSchema;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;
import nl.vu.datalayer.hbase.schema.HBasePredicateCFSchema;
import nl.vu.datalayer.hbase.util.HBHexastoreUtil;
import nl.vu.datalayer.hbase.util.HBPrefixMatchUtil;
import nl.vu.datalayer.hbase.util.HBasePredicateCFUtil;
import nl.vu.datalayer.hbase.connection.HBaseConnection;

import org.openrdf.model.Statement;

public class HBaseFactory {
	
	public static final String PREDICATE_CF= "predicate-cf";
	
	public static HBaseClientSolution getHBaseSolution(String schemaName, HBaseConnection con, ArrayList<Statement> statements) {
		if (schemaName.equals(PREDICATE_CF)){
			HBasePredicateCFSchema schema = new HBasePredicateCFSchema(con, statements);
			return new HBaseClientSolution(schema,
										new HBasePredicateCFUtil(con, schema));
		}
		else if (schemaName.equals(HBPrefixMatchSchema.SCHEMA_NAME)){
			HBPrefixMatchSchema schema = new HBPrefixMatchSchema(con);
			return new HBaseClientSolution(schema,
					new HBPrefixMatchUtil(con));
		}
		else{//default hexastore"
			HBHexastoreSchema schema = new HBHexastoreSchema(con);
			return new HBaseClientSolution(schema,
										new HBHexastoreUtil(con, schema));
		}
	}
}
