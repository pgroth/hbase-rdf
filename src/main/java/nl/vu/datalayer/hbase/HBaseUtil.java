package nl.vu.datalayer.hbase;

import java.util.ArrayList;
import java.util.Iterator;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.HConstants;

import com.sun.appserv.util.cache.Constants;


public class HBaseUtil {	
	
	public static HBaseAdmin hbase = null;
	public static Configuration conf = null;
	
	public HBaseUtil(String configFilePath) throws Exception
	{
		conf = HBaseConfiguration.create();
	    
		if (configFilePath == null) {
			System.out.println("got here");
			conf.set("hbase.master","localhost:60000");
	    }
		else {
			System.out.println("Yeah!!!");
			
		}
		
		System.out.println(conf);
		hbase = new HBaseAdmin(conf);
		
		
	}
	
	public void cachePredicates()  throws IOException {
	
	    
	    HTableDescriptor desc;
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
	
	public void createTableStruct(String table, ArrayList<String> columns)  throws IOException {

	    HTableDescriptor desc;
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
	
	public  void addRow(String tableName, String key, String columnFam, String columnName, String val) throws IOException {

	    
	    // add triples to HBase
	    HTable table = new HTable(conf, tableName);
	    Put row = new Put(Bytes.toBytes(key));
	    row.add(Bytes.toBytes(columnFam.replaceAll("[^A-Za-z0-9 ]", "")), Bytes.toBytes(columnName.replaceAll("[^A-Za-z0-9 ]", "")), Bytes.toBytes(val));
	    table.put(row);
	    
	    // store full predicate URI
	    String pred;
	    if (columnFam.compareTo("literal") != 0) {
	    	pred = columnFam;
	    }
	    else {
	    	pred = columnName;
	    }
	    
	    System.out.println("PRED ENTRY: " + pred + " " + pred.replaceAll("[^A-Za-z0-9 ]", ""));
	    table = new HTable(conf, "predicates");
	    row = new Put(Bytes.toBytes(pred.replaceAll("[^A-Za-z0-9 ]", ""))); 
	    row.add(Bytes.toBytes("URI"), Bytes.toBytes(""), Bytes.toBytes(pred));
	    table.put(row);
	}
	
	public ArrayList<ArrayList<String>> getRow(String URI, String tableName)  throws IOException {

	    HTable table = new HTable(conf, tableName);
	    
		Get g = new Get(Bytes.toBytes(URI));
	    Result r = table.get(g);
	    
	    ArrayList<ArrayList<String>> list = new ArrayList<ArrayList<String>>();
	    List<KeyValue> rawList = r.list();
	    
	    for (Iterator<KeyValue> it = rawList.iterator(); it.hasNext();) {
	    	KeyValue k = (KeyValue)it.next();
	    	ArrayList<String> triple = new ArrayList();
	    	
	    	String pred = Bytes.toString(k.getFamily());
	    	if (pred.compareTo("literal") == 0) {
	    		pred = Bytes.toString(k.getQualifier());
	    	}
	    	triple.add(pred);
	    	
	    	String val = Bytes.toString(k.getValue());
	    	triple.add(val);
	    	
	    	list.add(triple);
	    }
	    
	    return list;
	}
	
	public String getPredicate(String pred) throws IOException {
		String URI = "";
		
	    HTable table = new HTable(conf, "predicates");
	    
		Get g = new Get(Bytes.toBytes(pred));
	    Result r = table.get(g);
	    
	    List<KeyValue> rawList = r.list();
	    
	    for (Iterator<KeyValue> it = rawList.iterator(); it.hasNext();) {
	    	KeyValue k = (KeyValue)it.next();
	    	URI = Bytes.toString(k.getValue());
	    }
		
		return URI;
	}
}
