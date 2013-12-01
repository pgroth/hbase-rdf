package nl.vu.datalayer.hbase;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.operations.HBHexastoreOperationManager;
import nl.vu.datalayer.hbase.operations.HBasePredicateCFOperationManager;
import nl.vu.datalayer.hbase.operations.IHBaseOperationManager;
import nl.vu.datalayer.hbase.operations.prefixmatch.HBPrefixMatchOperationManager;
import nl.vu.datalayer.hbase.schema.HBHexastoreSchema;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;
import nl.vu.datalayer.hbase.schema.HBasePredicateCFSchema;
import nl.vu.datalayer.hbase.schema.IHBaseSchema;

import org.openrdf.model.Statement;

public class HBaseFactory {
	
	private static ConcurrentHashMap<Thread, HBaseClientSolution> hbaseOpsManagers = new ConcurrentHashMap<Thread, HBaseClientSolution>();
	private static ConcurrentHashMap<String, IHBaseSchema> schemaSet = new ConcurrentHashMap<String, IHBaseSchema>();
	
	public static HBaseClientSolution getHBaseSolution(String schemaName, HBaseConnection con, ArrayList<Statement> statements) { 
		HBaseClientSolution hbaseClientSol = hbaseOpsManagers.get(Thread.currentThread());
		if (hbaseClientSol != null){
			return hbaseClientSol;
		}
		
		synchronized (HBaseFactory.class) {	
			IHBaseSchema schema = schemaSet.get(schemaName);
			IHBaseOperationManager hbaseOpsManager;
			
			if (schemaName.endsWith(HBPrefixMatchSchema.SCHEMA_NAME)) {
				String schemaSuffix = getSchemaSuffix();
				if (schema == null) {
					if (schemaName.startsWith("local")) {
						schema = new HBPrefixMatchSchema(con, schemaSuffix, true, 0);//triples and 0 regions
					} else {
						schema = new HBPrefixMatchSchema(con, schemaSuffix);
					}
					schemaSet.put(schemaName, schema);
				}
				
				hbaseOpsManager = new HBPrefixMatchOperationManager(con);
			}
			else if (schemaName.equals(HBasePredicateCFSchema.SCHEMA_NAME)) {
				if (schema==null){
					schema = new HBasePredicateCFSchema(con, statements);
					schemaSet.put(schemaName, schema);
				}
				
				hbaseOpsManager = new HBasePredicateCFOperationManager(con);
				
			} else {//default hexastore"
				if (schema == null) {
					schema = new HBHexastoreSchema(con);
					schemaSet.put(schemaName, schema);
				}
				
				hbaseOpsManager = new HBHexastoreOperationManager(con, (HBHexastoreSchema) schema);
			}
			
			hbaseClientSol = new HBaseClientSolution(schema, hbaseOpsManager);
			hbaseOpsManagers.put(Thread.currentThread(), hbaseClientSol);
			return new HBaseClientSolution(schema, hbaseOpsManager);
		}
	}

	private static String getSchemaSuffix() {
		Properties prop = new Properties();
		try{
			prop.load(new FileInputStream("config.properties"));
		}
		catch (IOException e) {
			//continue to use the default properties
		}
		String schemaSuffix = prop.getProperty(HBPrefixMatchSchema.SUFFIX_PROPERTY, "");
		return schemaSuffix;
	}
	
	
	
}
