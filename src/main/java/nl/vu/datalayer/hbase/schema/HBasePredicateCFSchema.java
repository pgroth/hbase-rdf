package nl.vu.datalayer.hbase.schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.connection.NativeJavaConnection;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;


public class HBasePredicateCFSchema implements IHBaseSchema {	
	
	private ArrayList<Statement> statements;
	
	public static final String TABLE_NAME = "TRIPLETS";
	
	public static final String SCHEMA_NAME = "predicate-cf";
	
	private NativeJavaConnection con;
	
	public HBasePredicateCFSchema(HBaseConnection con, ArrayList<Statement> statements)
	{
		super();
		this.statements = statements;
		this.con = (NativeJavaConnection)con;
	}

	@Override
	public void create() throws Exception {
		// create table column families
		ArrayList<String> predicates = new ArrayList<String>();
		for (Iterator<Statement> iter = statements.iterator(); iter.hasNext();) {
			Statement s = iter.next();
			if (s.getObject() instanceof Resource) {
				predicates.add(s.getPredicate().stringValue());
			}
			else {
				// predicates.add("literal:" + s.getPredicate().stringValue());
			}
		}
		
		createTableStruct(TABLE_NAME, predicates);
		cachePredicates();	
	}
	
	private void cachePredicates()  throws IOException {
	    HTableDescriptor desc;
	    HBaseAdmin hbase = con.getAdmin();
	    if (hbase.tableExists("predicates") == false) {
	    	desc = new HTableDescriptor("predicates");
	    }
	    else {
		     desc = hbase.getTableDescriptor("predicates".getBytes());
	    }
	    
	    HColumnDescriptor c = new HColumnDescriptor("URI".getBytes());
	    desc.addFamily(c);

	    if (hbase.tableExists("predicates") == false) {
	    	hbase.createTable(desc);
	    }
	}
	
	private void createTableStruct(String table, ArrayList<String> columns)  throws IOException {

	    HTableDescriptor desc;
	    HBaseAdmin hbase = con.getAdmin();
	    if (hbase.tableExists(table) == false) {
	    	desc = new HTableDescriptor(table);
	    	HColumnDescriptor literal = new HColumnDescriptor("literal".getBytes());
	    	desc.addFamily(literal);
	    }
	    else {
		     desc = hbase.getTableDescriptor(table.getBytes());
	    }
	    
		for (Iterator<String> iter = columns.iterator(); iter.hasNext();) {
			String columnName = iter.next().replaceAll("[^A-Za-z0-9 ]", "");
//			System.out.println("COLUMN: " + columnName);
			
			HColumnDescriptor c = new HColumnDescriptor(columnName.getBytes());
			if (desc.hasFamily(columnName.getBytes()) == false) {
				desc.addFamily(c);
			}
		}

	    if (hbase.tableExists(table) == false) {
	    	hbase.createTable(desc);
	    }
	}	
}
