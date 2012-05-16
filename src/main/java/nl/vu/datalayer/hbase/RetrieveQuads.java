package nl.vu.datalayer.hbase;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

public class RetrieveQuads {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try{
			
			if (args.length != 1){
				System.out.println("Usage: RetrieveQuads <queryFile>");
				System.out.println("Use \"<?>\" for the positions representing variables");
				return;
			}
			
			HBaseConnection con = new HBaseConnection();
			
			HBaseClientSolution sol = HBaseFactory.getHBaseSolution(HBPrefixMatchSchema.SCHEMA_NAME, con, null);
			
			FileInputStream ifstream = new FileInputStream(args[0]);
			DataInputStream in = new DataInputStream(ifstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));

			String strLine;
			while ((strLine = br.readLine()) != null) {
				System.out.println("Query: "+strLine);
				//String[] quad = parseLine(strLine);
				long start = System.currentTimeMillis();
				ArrayList<ArrayList<String>> result = sol.util.getRow(strLine.split(" "));
				long end = System.currentTimeMillis();
				System.out.println("Result retrieved in: "+(end-start)+" ms");
				
				for (ArrayList<String> arrayList : result) {
					for (String string : arrayList) {
						System.out.print(string+" ");
					}
					System.out.println();
				}
			}
			
			con.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*private static String[] parseLine(String strLine) {
		int firstSpace = strLine.indexOf(" ");
		int secondSpace = strLine.indexOf(" ", firstSpace+1);
		int thirdSpace = strLine.indexOf(" ", secondSpace+1);
		String []ret = new String[4];
		ret[0] = strLine.substring(0, firstSpace);
		ret[1] = strLine.substring(firstSpace+1, secondSpace);
		ret[2] = strLine.substring(secondSpace+1, thirdSpace);
		return ret;
	}*/

}
