package nl.vu.datalayer.hbase.schema;

import java.io.IOException;

import nl.vu.datalayer.hbase.HBaseConnection;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

public class HBHexastoreSchema implements IHBaseSchema {

	private static Logger logger = Logger.getLogger("HexastoreLogger");

	public static final String SCHEMA_NAME = "hexastore";

	public static final int TABLE_COUNT = 7;

	public static final String[] TABLE_NAMES = { "O-SP", "P-SO", "PO-S", "S-PO", "SO-P", "SP-O", "SPO" };

	// the 1 bits in the index have to correspond to the positions which are
	// used as a key in S-P-O
	public static final int SPO = 7;
	public static final int S_PO = 4;
	public static final int P_SO = 2;
	public static final int O_SP = 1;
	public static final int SP_O = 6;
	public static final int SO_P = 5;
	public static final int PO_S = 3;

	public static final String COLUMN_FAMILY = "CF";
	public static final String COLUMN_NAME = "COL";

	// Encoding details
	public static final String TOKEN_DELIMITER = " ";// separates token within a
														// pair
														// e.g in a PO cell,
														// separates P from O
	public static final String PAIR_DELIMITER = "\n";// separates pairs e.g.
														// multiple PO pairs
														// within the same cell

	private HBaseConnection con;

	public HBHexastoreSchema(HBaseConnection con) {
		super();
		this.con = con;
		// PropertyConfigurator.configure(System.getProperty("log4j.configFile"));
	}

	private void createTable(HBaseAdmin admin, String tableName) throws IOException {
		HTableDescriptor desc = new HTableDescriptor(tableName);

		HColumnDescriptor famDesc = new HColumnDescriptor(COLUMN_FAMILY);
		desc.addFamily(famDesc);

		if (admin.tableExists(tableName) == false) {
			logger.info("Creating table: " + tableName);
			admin.createTable(desc);
		}
	}

	@Override
	public void create() throws Exception {
		HBaseAdmin admin = con.getAdmin();

		for (int i = 0; i < TABLE_NAMES.length; i++) {
			createTable(admin, TABLE_NAMES[i]);
		}
		logger.debug("Schema created");
	}

}
