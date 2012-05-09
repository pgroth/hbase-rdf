package nl.vu.datalayer.hbase;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import nl.vu.datalayer.hbase.schema.HBHexastoreSchema;

public class RetrieveTriples {

	public static void main(String[] arg) {
		try {

			if (arg.length != 1) {
				System.out.println("Usage: RetrieveTriples <queryFile>");
				System.out.println("Use \"<?>\" for the positions representing variables");
				return;
			}

			HBaseConnection con = new HBaseConnection();

			HBaseClientSolution sol = HBaseFactory.getHBaseSolution(HBHexastoreSchema.SCHEMA_NAME, con, null);

			FileInputStream ifstream = new FileInputStream(arg[0]);
			DataInputStream in = new DataInputStream(ifstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));

			String strLine;
			while ((strLine = br.readLine()) != null) {
				System.out.println("Query: " + strLine);
				String[] triple = strLine.split(" ");
				long start = System.currentTimeMillis();
				String result = sol.util.getRawCellValue(triple[0], triple[1], triple[2]);
				long end = System.currentTimeMillis();
				System.out.println("Result retrieved in: " + (end - start) + " ms");
				System.out.println(result);
			}

			con.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
