package nl.vu.datalayer.hbase;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;
import nl.vu.datalayer.hbase.util.IHBaseUtil;

import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.ntriples.NTriplesUtil;

public class RetrieveQuads {
	
	public static void recursiveResolveQuads(Value []valQuad, IHBaseUtil util) throws IOException{
		
		long start = System.currentTimeMillis();
		ArrayList<ArrayList<Value>> results = util.getResults(valQuad);
		long end = System.currentTimeMillis();
		
		/*ArrayList<Value> objects = new ArrayList<Value>();
		for (ArrayList<Value> arrayList : results ) {
			int i=0;
			for (Value val : arrayList) {
				System.out.print((val == null ? null : val.toString())+" ");
				if (i == 2){
					objects.add(val);
				}
				i++;
			}
			System.out.println();
		}*/
		System.out.println("Inner query: "+results.size()+" quads retrieved in: "+(end-start)+" ms");
		System.out.println("----------------");
		
		/*for (Value value : objects) {
			Value []newArray = {value, null, null, null};
			recursiveResolveQuads(newArray, util);
		}*/
		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try{
			
			if (args.length != 2){
				System.out.println("Usage: RetrieveQuads <queryFile> <Connection_type>");
				System.out.println("Use \"?\" for the positions representing variables");
				System.out.println("For Connection_type use: \"native-java\" or \"rest\"");
				return;
			}
			
			HBaseConnection con;
			if (args[1].equals("rest"))
				con = HBaseConnection.create(HBaseConnection.REST);
			else if (args[1].equals("native-java"))
				con = HBaseConnection.create(HBaseConnection.NATIVE_JAVA);
			else{
				System.err.println("Unknown HBase connection type");
				return;
			}	
			
			//the schemaSuffix is retrieved from config.properties
			HBaseClientSolution sol = HBaseFactory.getHBaseSolution(HBPrefixMatchSchema.SCHEMA_NAME, con, null);
			
			FileInputStream ifstream = new FileInputStream(args[0]);
			DataInputStream dataIn = new DataInputStream(ifstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(dataIn));

			String strLine;
			ValueFactory valFactory = new ValueFactoryImpl();
			
			//System.out.print(">");
			while ((strLine = br.readLine()) != null) {
				System.out.println("Query: "+strLine);
				
				//long start = System.currentTimeMillis();
				String []quad = strLine.split(" ");
				Value []valQuad = {null, null, null, null};
				for (int i = 0; i < valQuad.length; i++) {
					if (!quad[i].equals("?"))
						valQuad[i] = NTriplesUtil.parseValue(quad[i], valFactory);
				}
				try{
					recursiveResolveQuads(valQuad, sol.util);
				}
				catch (IOException e) {
					e.printStackTrace();
				}
				
				//long end = System.currentTimeMillis();
				//System.out.println("Outer loop: Quads retrieved in: "+(end-start)+" ms");
				
				System.out.println();
				System.out.print(">");
			}
			
			
			con.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static String[] parseLine(String strLine) {
		int firstSpace = strLine.indexOf(" ");
		int secondSpace = strLine.indexOf(" ", firstSpace+1);
		int lastSpace = strLine.lastIndexOf(" ");
		String []ret = new String[4];
		ret[0] = strLine.substring(0, firstSpace);
		ret[1] = strLine.substring(firstSpace+1, secondSpace);
		ret[2] = strLine.substring(secondSpace+1, lastSpace);
		ret[3] = strLine.substring(lastSpace+1);
		return ret;
	}

}
