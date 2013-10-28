package nl.vu.datalayer.hbase;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.operations.HBHexastoreOperationManager;
import nl.vu.datalayer.hbase.operations.HBPrefixMatchOperationManager;
import nl.vu.datalayer.hbase.operations.HBasePredicateCFOperationManager;
import nl.vu.datalayer.hbase.schema.HBHexastoreSchema;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;
import nl.vu.datalayer.hbase.schema.HBasePredicateCFSchema;

import org.openrdf.model.Statement;

public class HBaseFactory {
	
	public static HBaseClientSolution getHBaseSolution(String schemaName, HBaseConnection con, ArrayList<Statement> statements) {
		
		
		if (schemaName.equals(HBasePredicateCFSchema.SCHEMA_NAME)){
			HBasePredicateCFSchema schema = new HBasePredicateCFSchema(con, statements);
			return new HBaseClientSolution(schema,
										new HBasePredicateCFOperationManager(con));
		}
		else if (schemaName.endsWith(HBPrefixMatchSchema.SCHEMA_NAME)){
			Properties prop = new Properties();
			try{
				prop.load(new FileInputStream("config.properties"));
			}
			catch (IOException e) {
				//continue to use the default properties
			}
			String schemaSuffix = prop.getProperty(HBPrefixMatchSchema.SUFFIX_PROPERTY, "");
			
			HBPrefixMatchSchema schema;
			if (schemaName.startsWith("local")){
				schema = new HBPrefixMatchSchema(con, schemaSuffix, true, 0);//triples and 0 regions
			}
			else{
				schema = new HBPrefixMatchSchema(con, schemaSuffix);
			}
			
			return new HBaseClientSolution(schema,
					new HBPrefixMatchOperationManager(con));
		}
		else{//default hexastore"
			HBHexastoreSchema schema = new HBHexastoreSchema(con);
			return new HBaseClientSolution(schema,
										new HBHexastoreOperationManager(con, schema));
		}
	}
}
