package nl.vu.datalayer.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

//import com.sun.appserv.util.cache.Constants;

public class HBaseUtil {

	public static HBaseAdmin hbase = null;
	public static Configuration conf = null;

	public static final String ZOOKEEPER_QUORUM = "das3001.cm.cluster,das3002.cm.cluster,das3003.cm.cluster,das3004.cm.cluster,das3005.cm.cluster";

	public HBaseUtil(String configFilePath) throws Exception {
		conf = HBaseConfiguration.create();

		if (configFilePath == null) {
			// conf.set("hbase.master","fs0.cm.cluster:60000");
			conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
		} else {
			System.out.println("Yeah!!!");

		}

		System.out.println(conf);
		hbase = new HBaseAdmin(conf);

	}

	public void cachePredicates() throws IOException {

		HTableDescriptor desc;
		if (hbase.tableExists("predicates") == false) {
			desc = new HTableDescriptor("predicates");
		} else {
			desc = hbase.getTableDescriptor("predicates".getBytes());
		}

		HColumnDescriptor c = new HColumnDescriptor("URI".getBytes());
		desc.addFamily(c);

		if (hbase.tableExists("predicates") == false) {
			hbase.createTable(desc);
		}
	}

	public void createTableStruct(String table, ArrayList<String> columns) throws IOException {

		HTableDescriptor desc;
		if (hbase.tableExists(table) == false) {
			desc = new HTableDescriptor(table);
			HColumnDescriptor literal = new HColumnDescriptor("literal".getBytes());
			desc.addFamily(literal);
		} else {
			desc = hbase.getTableDescriptor(table.getBytes());
		}

		for (Iterator<String> iter = columns.iterator(); iter.hasNext();) {
			String columnName = iter.next().replaceAll("[^A-Za-z0-9 ]", "");
			// System.out.println("COLUMN: " + columnName);

			HColumnDescriptor c = new HColumnDescriptor(columnName.getBytes());
			if (desc.hasFamily(columnName.getBytes()) == false) {
				desc.addFamily(c);
			}
		}

		if (hbase.tableExists(table) == false) {
			hbase.createTable(desc);
		}
	}

	public void addRow(String tableName, String key, String columnFam, String columnName, String val)
			throws IOException {

		// add triples to HBase
		HTable table = new HTable(conf, tableName);
		Put row = new Put(Bytes.toBytes(key));
		row.add(Bytes.toBytes(columnFam.replaceAll("[^A-Za-z0-9 ]", "")),
				Bytes.toBytes(columnName.replaceAll("[^A-Za-z0-9 ]", "")), Bytes.toBytes(val));
		table.put(row);
		table.close();

		// store full predicate URI
		String pred;
		if (columnFam.compareTo("literal") != 0) {
			pred = columnFam;
		} else {
			pred = columnName;
		}

		System.out.println("PRED ENTRY: " + pred + " " + pred.replaceAll("[^A-Za-z0-9 ]", ""));
		table = new HTable(conf, "predicates");
		row = new Put(Bytes.toBytes(pred.replaceAll("[^A-Za-z0-9 ]", "")));
		row.add(Bytes.toBytes("URI"), Bytes.toBytes(""), Bytes.toBytes(pred));
		table.put(row);
		table.close();
	}

	public ArrayList<ArrayList<String>> getRow(String URI, String tableName) throws IOException {

		HTable table = new HTable(conf, tableName);

		Get g = new Get(Bytes.toBytes(URI));
		Result r = table.get(g);

		ArrayList<ArrayList<String>> list = new ArrayList<ArrayList<String>>();
		List<KeyValue> rawList = r.list();

		for (Iterator<KeyValue> it = rawList.iterator(); it.hasNext();) {
			KeyValue k = it.next();
			ArrayList<String> triple = new ArrayList<String>();

			String pred = Bytes.toString(k.getFamily());
			if (pred.compareTo("literal") == 0) {
				pred = Bytes.toString(k.getQualifier());
			}
			triple.add(pred);

			String val = Bytes.toString(k.getValue());
			triple.add(val);

			list.add(triple);
		}

		table.close();

		return list;
	}

	public String getPredicate(String pred) throws IOException {
		String URI = "";

		HTable table = new HTable(conf, "predicates");

		Get g = new Get(Bytes.toBytes(pred));
		Result r = table.get(g);

		List<KeyValue> rawList = r.list();

		for (Iterator<KeyValue> it = rawList.iterator(); it.hasNext();) {
			KeyValue k = it.next();
			URI = Bytes.toString(k.getValue());
		}
		table.close();

		return URI;
	}
}
