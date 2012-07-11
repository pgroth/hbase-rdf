package nl.vu.datalayer.hbase;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.schema.HBHexastoreSchema;

public class RetrieveTriples {

	public static void main(String[] arg) {
		try {

			if (arg.length != 1) {
				System.out.println("Usage: RetrieveTriples <queryFile>");
				System.out.println("Use \"<?>\" for the positions representing variables");
				return;
			}

			HBaseConnection con = HBaseConnection.create(HBaseConnection.NATIVE_JAVA);

			HBaseClientSolution sol = HBaseFactory.getHBaseSolution(HBHexastoreSchema.SCHEMA_NAME, con, null);

			FileInputStream ifstream = new FileInputStream(arg[0]);
			DataInputStream in = new DataInputStream(ifstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));

			String strLine;
			while ((strLine = br.readLine()) != null) {
				System.out.println("Query: " + strLine);
				String[] triple = parseLine(strLine);
				long start = System.currentTimeMillis();
				String result = sol.util.getRawCellValue(triple[0], triple[1], triple[2]);
				long end = System.currentTimeMillis();
				System.out.println("Result retrieved in: " + (end - start) + " ms");
				System.out.println(result);
			}
			br.close();
			con.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static String[] parseLine(String strLine) {
		int firstSpace = strLine.indexOf(" ");
		int secondSpace = strLine.indexOf(" ", firstSpace + 1);
		String[] ret = new String[3];
		ret[0] = strLine.substring(0, firstSpace);
		ret[1] = strLine.substring(firstSpace + 1, secondSpace);
		ret[2] = strLine.substring(secondSpace + 1);
		return ret;
	}
}
