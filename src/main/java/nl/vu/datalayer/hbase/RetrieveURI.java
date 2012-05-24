package nl.vu.datalayer.hbase;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;

import nl.vu.datalayer.hbase.schema.HBasePredicateCFSchema;
import nl.vu.datalayer.hbase.schema.IHBaseSchema;
import nl.vu.datalayer.hbase.util.HBasePredicateCFUtil;
import nl.vu.datalayer.hbase.connection.HBaseConnection;


public class RetrieveURI {
	
	public static void retrieveURI(String URI, BufferedWriter out) {
		try {
			HBaseConnection con = HBaseConnection.create(HBaseConnection.NATIVE_JAVA);
			IHBaseSchema schema  = new HBasePredicateCFSchema(con, null);
			HBasePredicateCFUtil util = new HBasePredicateCFUtil(con, schema);
			
			String []triplet = {URI, null, null};
			ArrayList<ArrayList<String>> triples = util.getRow(triplet);
			
			for (Iterator<ArrayList<String>> it = triples.iterator(); it.hasNext();) {
				ArrayList<String> triple = (ArrayList<String>)it.next();
				int index = 0;
				
				out.write("<" + URI + "> ");
				
				for (Iterator<String> jt = triple.iterator(); jt.hasNext();) {
					String res = (String)jt.next();
					index++;
					
					if (index == 1) {
						res = util.getPredicate(res);
					}
					
					if (index < 2) {
						out.write("<" + res + "> ");
					}
					else {
						out.write(res);
					}
				}
				out.write(".\n");
			}
			
			out.close();
		}
		catch (Exception e) {
		}
	}
	
	public static void printURIInfo(String URI) {
		try {
			HBaseConnection con = HBaseConnection.create(HBaseConnection.NATIVE_JAVA);
			IHBaseSchema schema  = new HBasePredicateCFSchema(con, null);
			HBasePredicateCFUtil util = new HBasePredicateCFUtil(con, schema);
			
			String []triplet = {URI, null, null};
			ArrayList<ArrayList<String>> triples = util.getRow(triplet);
			
			for (Iterator<ArrayList<String>> it = triples.iterator(); it.hasNext();) {
				ArrayList<String> triple = (ArrayList<String>)it.next();
				int index = 0;
				
				System.out.println("<" + URI + "> ");
				
				for (Iterator<String> jt = triple.iterator(); jt.hasNext();) {
					String res = (String)jt.next();
					index++;
					
					if (index == 1) {
						res = util.getPredicate(res);
					}
					
					if (index < 2) {
						System.out.println("<" + res + "> ");
					}
					else {
						System.out.println(res);
					}
				}
				System.out.println(".\n");
			}
			
		}
		catch (Exception e) {
		}
	}
	
	public static void retrieveFile(String inFile, String outFile, String table) {
		try {
			  FileInputStream ifstream = new FileInputStream(inFile);
			  DataInputStream in = new DataInputStream(ifstream);
			  BufferedReader br = new BufferedReader(new InputStreamReader(in));
			  
			  FileWriter ofstream = new FileWriter(outFile);
			  BufferedWriter out = new BufferedWriter(ofstream);
			  
			  String strLine;
			  while ((strLine = br.readLine()) != null)   {
				  retrieveURI(strLine, out);
			  }
			  
			  in.close();
			  out.close();
		}
		catch (Exception e) {
			  System.err.println("Error: " + e.getMessage());
		}
	}
	
	public static void main(String[] args) {
		printURIInfo("http://www.w3.org/data#W3C");
		//retrieveFile("/home/anca/Documents/OPS/trials/URIlist", "/home/anca/Documents/OPS/trials/out.ttl", "excerpt");
	}
}
