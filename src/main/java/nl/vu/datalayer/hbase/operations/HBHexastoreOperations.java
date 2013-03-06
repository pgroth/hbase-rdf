package nl.vu.datalayer.hbase.operations;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;

import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.schema.HBHexastoreSchema;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;

public class HBHexastoreOperations implements IHBaseOperations {

	private HBaseConnection con;
	private HBHexastoreSchema schema;

	private static Logger logger = Logger.getLogger("HexastoreLogger");

	public static final int FLUSH_LIMIT = 100;

	private ArrayList<ArrayList<Integer>> keyPositions;
	private ArrayList<ArrayList<Integer>> valuePositions;

	public HBHexastoreOperations(HBaseConnection con, HBHexastoreSchema schema) {
		super();
		this.con = con;
		this.schema = schema;
		keyPositions = new ArrayList<ArrayList<Integer>>();
		valuePositions = new ArrayList<ArrayList<Integer>>();
		computeKeyAndValuePositions();
	}

	private void computeKeyAndValuePositions(int index, ArrayList<Integer> keyPosition, ArrayList<Integer> valuePosition) {
		// the elements from the triple S-P-O that make up the key correspond to
		// the 1 bits in the index parameter
		// e.g. if index = 5 (binary 101) then the key is SO and the value is P
		// if index = 4 (binary 100) then the key is S and the values is PO

		int position = 0;
		for (int i = 0; i < 3; i++) {// assuming the index is at most 7
			if ((index & 1) == 1) {
				keyPosition.add(position);
			} else {
				valuePosition.add(position);
			}
			index >>= 1;
			position++;
		}
	}

	private void computeKeyAndValuePositions() {
		for (int i = 0; i < HBHexastoreSchema.TABLE_COUNT; i++) {
			int bitCount = Integer.bitCount(i + 1);

			ArrayList<Integer> keyPosition = new ArrayList<Integer>(bitCount);
			ArrayList<Integer> valuePosition = new ArrayList<Integer>(3 - bitCount);
			computeKeyAndValuePositions(i + 1, keyPosition, valuePosition);

			logger.debug("Table: " + HBHexastoreSchema.TABLE_NAMES[i] + "Index: " + Integer.toString(i + 1)
					+ "; BitCount: " + Integer.toString(bitCount));
			logger.debug("KeyPositions: ");
			for (int j = 0; j < keyPosition.size(); j++) {
				logger.debug("KeyPosition: " + keyPosition.get(j));
			}
			logger.debug("ValuePositions: ");
			for (int j = 0; j < valuePosition.size(); j++) {
				logger.debug("valPosition: " + valuePosition.get(j));
			}
			keyPositions.add(keyPosition);
			valuePositions.add(valuePosition);
		}
	}

	/**
	 * Hash function to convert any string to an 8-byte integer
	 * 
	 * @param key
	 * @return
	 */
	private long hashFunction(String key) {
		long hash = 0;

		for (int i = 0; i < key.length(); i++) {
			hash = key.charAt(i) + (hash << 6) + (hash << 16) - hash;
		}

		return hash;
	}

	@Override
	public ArrayList<ArrayList<String>> getResults(String[] triple) throws IOException {

		String[] inversedTriple = new String[3];
		inversedTriple[0] = triple[2].equals("?") ? null : triple[2];
		inversedTriple[1] = triple[1].equals("?") ? null : triple[1];
		inversedTriple[2] = triple[0].equals("?") ? null : triple[0];

		int index = 0;
		for (int i = 0; i < inversedTriple.length; i++) {
			if (inversedTriple[i] != null) {
				index |= 1 << i;
			}
		}

		byte[] key = getKey(inversedTriple, index - 1);

		Get g = new Get(key);
		HTableInterface table = con.getTable(HBHexastoreSchema.TABLE_NAMES[index - 1]);
		Result r = table.get(g);

		byte[] value = r.getValue(HBHexastoreSchema.COLUMN_FAMILY.getBytes(), HBHexastoreSchema.COLUMN_NAME.getBytes());
		ArrayList<ArrayList<String>> ret = new ArrayList<ArrayList<String>>();
		if (value != null){
			ArrayList<String> first = new ArrayList<String>();
			first.add(Bytes.toString(value));
			ret.add(first);
		}
		return ret;
	}

	/**
	 * Generate an HBase key from a triple and a table index see
	 * HBHexastoreSchema for the associations between indexes and tables
	 * 
	 * @param triple
	 * @param index
	 * @return
	 */
	public byte[] getKey(String[] triple, int index) {
		ArrayList<Integer> keyMask = keyPositions.get(index);

		// construct the key by concatenating the hashes of each element
		long[] hashes = new long[keyMask.size()];
		for (int i = 0; i < hashes.length; i++) {
			hashes[i] = hashFunction(triple[keyMask.get(i)]);
		}

		byte[] key;
		switch (hashes.length) {
		case 3:
			key = Bytes.add(Bytes.toBytes(hashes[2]), Bytes.toBytes(hashes[1]), Bytes.toBytes(hashes[0]));
			break;
		case 2:
			key = Bytes.add(Bytes.toBytes(hashes[1]), Bytes.toBytes(hashes[0]));
			break;
		case 1:
			key = Bytes.toBytes(hashes[0]);
			break;
		default:
			throw new RuntimeException("Unexpected number of bits");
		}

		return key;
	}

	/**
	 * Generate an HBase cell value from a triple and a table index see
	 * HBHexastoreSchema for the associations between indexes and tables
	 * 
	 * @param triple
	 * @param index
	 * @return
	 */
	public byte[] getValue(String[] triple, int index) {
		ArrayList<Integer> valueMask = valuePositions.get(index);

		String value = "";
		if (valueMask.size() > 0) {
			value += triple[valueMask.get(valueMask.size() - 1)];
			if (valueMask.size() > 1) {
				value += HBHexastoreSchema.TOKEN_DELIMITER + triple[valueMask.get(0)];
			}
			value += HBHexastoreSchema.PAIR_DELIMITER;
		}
		logger.debug("Value: " + value);
		return value.getBytes();
	}

	/**
	 * @param tables
	 * @param index
	 *            - should be between 1 and 7
	 * @param triple
	 * @param batchPut
	 * @throws IOException
	 */
	private boolean addTriple(HTable[] tables, int index, String[] triple) throws IOException {

		byte[] key = getKey(triple, index);

		logger.debug("Table: " + HBHexastoreSchema.TABLE_NAMES[index] + "; Key: "
				+ new BigInteger(key).abs().toString(16));

		// TODO has to be changed with Append when hbase 0.93 becomes stable
		// for now we do a Get and append to the retrieved value
		Get g = new Get(key);
		Result r = tables[index].get(g);
		// System.out.println(r);
		byte[] val;
		byte[] existingValue = r.getValue(HBHexastoreSchema.COLUMN_FAMILY.getBytes(),
				HBHexastoreSchema.COLUMN_NAME.getBytes());
		if (existingValue != null && index == (HBHexastoreSchema.SPO - 1)) {
			System.out.println("Existing value detected in SPO");
			// this is a duplicate triple, we don't want to add it anymore
			return false;
		}

		// construct the String value
		byte[] value = getValue(triple, index);

		if (existingValue != null) {
			val = Bytes.add(existingValue, value);
		} else {
			val = value;
		}

		Put p = new Put(key);
		p.setWriteToWAL(false);
		p.add(HBHexastoreSchema.COLUMN_FAMILY.getBytes(), HBHexastoreSchema.COLUMN_NAME.getBytes(), val);
		tables[index].put(p);
		return true;
		// batchPut.add(p);

		// Append append = new Append(key.toByteArray());
	}

	@Override
	public void populateTables(ArrayList<Statement> statements) throws Exception {

		HTable[] tables = new HTable[HBHexastoreSchema.TABLE_COUNT];
		// ArrayList<ArrayList<Put>> batchPuts = new
		// ArrayList<ArrayList<Put>>();
		// initialize connections to all tables
		// for each table initialize a buffer of Put operations
		for (int i = 0; i < tables.length; i++) {
			tables[i] = (HTable) con.getTable(HBHexastoreSchema.TABLE_NAMES[i]);
			tables[i].setAutoFlush(false);
			// batchPuts.add(new ArrayList<Put>());
		}

		// populate tables
		long start = System.currentTimeMillis();
		for (int i = 0; i < statements.size(); i++) {
			Statement statement = statements.get(i);
			logger.debug("Triple: " + statement.toString());

			String[] triple = new String[3];
			triple[0] = statement.getObject().toString();
			triple[1] = statement.getPredicate().toString();
			triple[2] = statement.getSubject().toString();

			// first check if the triple is added in the SPO table
			// if it isn't added then it is a duplicate, so we don't add it to
			// other tables anymore
			boolean added = addTriple(tables, HBHexastoreSchema.SPO - 1, triple);
			if (added == true) {// add it also to the other tables
				for (int j = 0; j < tables.length - 1; j++) {
					addTriple(tables, j, triple);
				}
			}

			// flush the buffered Put operations
			/*
			 * if ((i+1) % FLUSH_LIMIT == 0){ for (int j = 0; j < tables.length;
			 * j++) { tables[j].put(batchPuts.get(j)); batchPuts.get(j).clear();
			 * } }
			 */
		}

		long end = System.currentTimeMillis();
		System.out.println("Total loading time: " + (end - start));

		// do a last flush with the remaining buffered Puts
		/*
		 * for (int j = 0; j < tables.length; j++) {
		 * tables[j].put(batchPuts.get(j)); }
		 */
	}

	@Override
	public ArrayList<ArrayList<Value>> getResults(Value[] quad) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * nl.vu.datalayer.hbase.util.IHBaseUtil#countResults(org.openrdf.model.
	 * Value[], long)
	 */
	@Override
	public long countResults(Value[] quad, long hardLimit) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

}
