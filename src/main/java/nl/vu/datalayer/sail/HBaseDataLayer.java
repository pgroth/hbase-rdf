package nl.vu.datalayer.sail;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class HBaseDataLayer 
{
	// Logger
		protected static Logger logger = LoggerFactory.getLogger(HBaseDataLayer.class);

		// Prefixes
		private static final byte[] COLUMN = "a".getBytes();
		private static final byte[] COUNT_QUALIFIER = "c".getBytes();
		private static final byte[] EMPTY_BYTEARRAY = new byte[0];
		private static final byte[] EXISTENCE_QUALIFIER = "e".getBytes();
		private static final byte[] RESOURCE_QUALIFIER = "r".getBytes();

		// HBase interface
		private HBaseAdmin admin;

		// A random number generator
		protected final Random random = new Random();

		// Count tables
		protected HTable sp_counts;
		protected HTable so_counts;
		protected HTable po_counts;

		// Data tables
		protected HTable sp_data;
		protected HTable so_data;
		protected HTable po_data;

		// All triples table
		protected HTable spo_data;

		/**
		 * @throws IOException
		 */
		public HBaseDataLayer() throws IOException {
			// Get configuration from the path
			admin = new HBaseAdmin(new HBaseConfiguration());

			initialiseTables();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see nl.erdf.datalayer.DataLayer#clear()
		 */
		public void clear() {
			logger.info("Clearing tables");
			shutdown();
			try {
				disableAndDeleteTable(sp_data);
				disableAndDeleteTable(po_data);
				disableAndDeleteTable(so_data);

				disableAndDeleteTable(sp_counts);
				disableAndDeleteTable(po_counts);
				disableAndDeleteTable(so_counts);

				disableAndDeleteTable(spo_data);

				initialiseTables();

			} catch (IOException e) {
				logger.error("Could not clear tables", e);
			}
			logger.info("Tables cleared");
		}

		/**
		 * @param a
		 * @param b
		 * @return
		 */
		private byte[] concat(byte[] a, byte[] b) {
			byte[] ret = new byte[a.length + b.length];
			System.arraycopy(a, 0, ret, 0, a.length);
			System.arraycopy(b, 0, ret, a.length, b.length);
			return ret;
		}

		/**
		 * @param tableName
		 * @throws IOException
		 */
		protected void createTable(String tableName) throws IOException {
			HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
			hTableDescriptor.addFamily(new HColumnDescriptor(COLUMN));
			admin.createTable(hTableDescriptor);
			logger.info("Created table: " + tableName);
		}

		/**
		 * @param table
		 * @throws IOException
		 */
		private void disableAndDeleteTable(HTable table) throws IOException {
			admin.disableTable(table.getTableName());
			admin.deleteTable(table.getTableName());
		}

		/**
		 * @pre One and only one WILDCARD in the QueryPattern
		 * @param queryPattern
		 * @return
		 */
		private HTable getCountTable(Triple queryPattern) {
			HTable t;
			if (queryPattern.getSubject().equals(Node.ANY)) {
				t = po_counts;
			} else if (queryPattern.getPredicate().equals(Node.ANY)) {
				t = so_counts;
			} else if (queryPattern.getObject().equals(Node.ANY)) {
				t = sp_counts;
			} else
				throw new IllegalArgumentException("Query pattern must have at least one wildcard");
			return t;
		}

		/**
		 * @pre One and only one WILDCARD in the QueryPattern
		 * @param queryPattern
		 * @return
		 */
		private HTable getDataTable(Triple queryPattern) {
			HTable t;
			if (queryPattern.getSubject().equals(Node.ANY)) {
				t = po_data;
			} else if (queryPattern.getPredicate().equals(Node.ANY)) {
				t = so_data;
			} else if (queryPattern.getObject().equals(Node.ANY)) {
				t = sp_data;
			} else
				throw new IllegalArgumentException("Query pattern must have at least one wildcard");
			return t;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * nl.erdf.datalayer.DataLayer#getNumberOfResources(com.hp.hpl.jena.graph
		 * .Triple)
		 */
		public long getNumberOfResources(Triple queryPattern) {
			HTable t = getCountTable(queryPattern);

			Get g = new Get(queryPatternToByteArray(queryPattern));
			try {
				Result r = t.get(g);
				byte[] c = r.getValue(COLUMN, COUNT_QUALIFIER);
				if (c == null)
					return 0;
				else {
					long ret = Bytes.toLong(c);
					assert ret > 0 : ret;
					return ret;
				}
			} catch (IOException e) {
				logger.error("Could not get counts", e);
				return -1;
			}
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * nl.erdf.datalayer.DataLayer#getResources(com.hp.hpl.jena.graph.Triple)
		 */
		public Set<Node> getResources(Triple pattern) {
			Set<Node> nodes = new HashSet<Node>();

			// FIXME: currently only returns one resource at random
			try {
				byte[] query = queryPatternToByteArray(pattern);

				long numberOfResources = getNumberOfResources(pattern);
				if (numberOfResources == 0)
					return null; // Nothing matches this pattern
				// FIXME: optimize: reuse computation
				long index = random.nextInt((int) numberOfResources);

				byte[] queryWithRandom = new byte[query.length + 8];
				System.arraycopy(query, 0, queryWithRandom, 0, query.length);
				Bytes.putLong(queryWithRandom, query.length, index);

				Get g = new Get(queryWithRandom);

				HTable t = getDataTable(pattern);

				Result r = t.get(g);
				byte[] c = r.getValue(COLUMN, RESOURCE_QUALIFIER);

				if (logger.isDebugEnabled())
					logger.debug("Getting " + Arrays.toString(queryWithRandom) + " from table "
							+ t.getTableDescriptor().getNameAsString() + " -> " + (c == null ? "null" : Arrays.toString(c)));

				if (c != null)
					nodes.add(NodeSerializer.fromBytes(c));
			} catch (IOException e) {
				logger.error("Could not perform scan", e);
			}

			return nodes;
		}

		/**
		 * @throws IOException
		 */
		protected void initialiseTables() throws IOException {
			String[] tableNames = new String[] { "sp_data", "po_data", "so_data", "sp_counts", "po_counts", "so_counts",
					"spo_data" };
			for (String n : tableNames) {
				if (!admin.tableExists(n))
					createTable(n);
			}
			sp_data = new HTable("sp_data");
			po_data = new HTable("po_data");
			so_data = new HTable("so_data");

			sp_counts = new HTable("sp_counts");
			po_counts = new HTable("po_counts");
			so_counts = new HTable("so_counts");

			spo_data = new HTable("spo_data");
			logger.info("Tables initialised");
		}

		/**
		 * Low-level insert operation for a table. Only inserts on the data table.
		 * Inserts an empty value
		 * 
		 * @param key
		 * @param table
		 * @param value
		 * @throws IOException
		 */
		protected void insert(byte[] key, HTable data_table) throws IOException {
			Put put = new Put(key);
			put.add(COLUMN, EXISTENCE_QUALIFIER, EMPTY_BYTEARRAY);
			data_table.put(put);
		}

		/**
		 * Low-level insert operation for a table. Increments on the count table and
		 * inserts on the data table.
		 * 
		 * @param key
		 * @param table
		 * @param value
		 * @throws IOException
		 */
		protected void insert(byte[] key, HTable data_table, HTable count_table, byte[] value) throws IOException {
			long c = count_table.incrementColumnValue(key, COLUMN, COUNT_QUALIFIER, 1, false) - 1; // Minus
																									// one
																									// because
																									// it
																									// returns
																									// post-increment
			if (c >= Integer.MAX_VALUE) {
				logger.error("Too many values for key " + Arrays.toString(key) + " for table " + count_table);
			}

			byte[] nKey = new byte[key.length + 8];
			System.arraycopy(key, 0, nKey, 0, key.length);
			Bytes.putLong(nKey, key.length, c);

			Put put = new Put(nKey);
			put.add(COLUMN, RESOURCE_QUALIFIER, value);

			if (logger.isDebugEnabled())
				logger.debug("Inserting " + Arrays.toString(nKey) + " -> " + Arrays.toString(value) + " to table "
						+ data_table.getTableDescriptor().getNameAsString());
			data_table.put(put);
		}

		/**
		 * @param sub
		 * @param pred
		 * @param obj
		 */
		public void insert(Value sub, Value pred, Value obj) {
			try {
				if (logger.isDebugEnabled())
					logger.debug("Inserting: " + sub + " " + pred + " " + obj);

				// FIXME: Can be made more compact by using resourcesToBytes
				byte[] s = NodeSerializer.toBytes(sub);
				byte[] p = NodeSerializer.toBytes(pred);
				byte[] o = NodeSerializer.toBytes(obj);

				byte[] sp = concat(s, p);
				byte[] po = concat(p, o);
				byte[] so = concat(s, o);
				byte[] spo = concat(s, concat(p, o));

				insert(sp, sp_data, sp_counts, o);
				insert(po, po_data, po_counts, s);
				insert(so, so_data, so_counts, p);
				insert(spo, spo_data);

			} catch (IOException e) {
				logger.error("Could not insert triple", e);
			}
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see nl.erdf.datalayer.DataLayer#isValid(com.hp.hpl.jena.graph.Triple)
		 */
		public boolean isValid(Triple triple) {
			try {
				Get g = new Get(resourcesToBytes(triple.getSubject(), triple.getPredicate(), triple.getObject()));
				Result res = spo_data.get(g); // FIXME: make sure that an empty byte
												// array is a good value

				return !res.isEmpty();
			} catch (IOException e) {
				logger.error("Could not check existence for " + triple, e);
				return false;
			}
		}

		/**
		 * Converts a query pattern to a byte array. FIXME: optimise: reduce object
		 * allocations
		 * 
		 * @param pattern
		 * @return
		 */
		protected byte[] queryPatternToByteArray(Triple pattern) {
			if (pattern.getSubject().equals(Node.ANY))
				return resourcesToBytes(pattern.getPredicate(), pattern.getObject());
			else if (pattern.getPredicate().equals(Node.ANY))
				return resourcesToBytes(pattern.getSubject(), pattern.getObject());
			else if (pattern.getObject().equals(Node.ANY))
				return resourcesToBytes(pattern.getSubject(), pattern.getPredicate());
			else
				throw new IllegalArgumentException("Query pattern has no wildcards: " + pattern);
		}

		/**
		 * @param r
		 * @return
		 */
		private byte[] resourcesToBytes(Node... r) {
			byte[][] ba = new byte[r.length][];
			int count = 0;

			for (int i = 0; i < ba.length; i++) {
				ba[i] = NodeSerializer.toBytes(r[i]);
				count += ba[i].length;
			}

			byte[] ret = new byte[count];
			count = 0;
			for (int i = 0; i < ba.length; i++) {
				System.arraycopy(ba[i], 0, ret, count, ba[i].length);
				count += ba[i].length;
			}
			return ret;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see nl.erdf.datalayer.DataLayer#shutdown()
		 */
		public void shutdown() {
			try {
				sp_data.close();
				po_data.close();
				so_data.close();

				sp_counts.close();
				po_counts.close();
				so_counts.close();

				spo_data.close();
			} catch (IOException e) {
				logger.error("could not close tables", e);
				throw new IllegalStateException(e);
			}
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see nl.erdf.datalayer.DataLayer#waitForLatencyBuffer()
		 */
		public void waitForLatencyBuffer() {
			// Just sleep for a few milliseconds
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		public byte[] serialize()
}
