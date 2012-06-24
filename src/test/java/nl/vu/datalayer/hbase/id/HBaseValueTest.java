package nl.vu.datalayer.hbase.id;


import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.ntriples.NTriplesUtil;


public class HBaseValueTest {
	
	private ValueFactory valFact;
	
	@Before
	public void runBeforeEveryTest() {
		 valFact = new ValueFactoryImpl();
	}

	@Test
	public void testURI(){
		try {
			Value inputURI = new URIImpl("http://dbpedia.org/resource/Alabama");
			HBaseValue inputVal = new HBaseValue(inputURI);
			
			ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
			inputVal.write(new DataOutputStream(out));
			
			ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
			HBaseValue outputVal = new HBaseValue();
			outputVal.readFields(new DataInputStream(in));
			
			Value underlying = outputVal.getUnderlyingValue();
			
			assertTrue(underlying instanceof URI);
				
			assertTrue(underlying.equals(inputURI));
			System.out.println(outputVal);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testBNode(){
		try {
			Value inputBNode = new BNodeImpl("bnodeIdTest");
			HBaseValue inputVal = new HBaseValue(inputBNode);
			
			ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
			inputVal.write(new DataOutputStream(out));
			
			ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
			HBaseValue outputVal = new HBaseValue();
			outputVal.readFields(new DataInputStream(in));
			
			Value underlying = outputVal.getUnderlyingValue();
			
			assertTrue(underlying instanceof BNode);
				
			assertTrue(underlying.equals(inputBNode));
			System.out.println(outputVal);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testPlainLiteral(){
		try {
			Value inputLiteral = new LiteralImpl("This a Test Plain Literal");
			
			HBaseValue inputVal = new HBaseValue(inputLiteral);
			
			ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
			inputVal.write(new DataOutputStream(out));
			
			ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
			HBaseValue outputVal = new HBaseValue();
			outputVal.readFields(new DataInputStream(in));
			
			Value underlying = outputVal.getUnderlyingValue();
			
			assertTrue(underlying instanceof Literal);
				
			assertTrue(underlying.equals(inputLiteral));
			System.out.println(outputVal);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testLongPlainLiteral(){
		try {
			String inputString = "";
			for (int i = 0; i < 100000; i++) {
				inputString += "b";
			}
			Value inputLiteral = new LiteralImpl(inputString);
			
			HBaseValue inputVal = new HBaseValue(inputLiteral);
			
			ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
			inputVal.write(new DataOutputStream(out));
			
			ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
			HBaseValue outputVal = new HBaseValue();
			outputVal.readFields(new DataInputStream(in));
			
			Value underlying = outputVal.getUnderlyingValue();
			
			assertTrue(underlying instanceof Literal);
				
			assertTrue(underlying.equals(inputLiteral));
			System.out.println(outputVal);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void testPlainLiteralNonASCII(){
		try {
			Value inputLiteral = NTriplesUtil.parseValue("\"\\\"Kucheriya\\\"popplace = \u2022  \u2022>\"", valFact);
			HBaseValue inputVal = new HBaseValue(inputLiteral);
			
			ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
			inputVal.write(new DataOutputStream(out));
			
			ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
			HBaseValue outputVal = new HBaseValue();
			outputVal.readFields(new DataInputStream(in));
			
			Value underlying = outputVal.getUnderlyingValue();
			
			assertTrue(underlying instanceof Literal);
				
			assertTrue(underlying.equals(inputLiteral));
			System.out.println(outputVal);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testDatatypeLiteral(){
		try {
			Value inputLiteral = new LiteralImpl("This a Test Datatype Literal",
												new URIImpl("http://www.w3.org/2001/XMLSchema#double"));
			HBaseValue inputVal = new HBaseValue(inputLiteral);
			
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			inputVal.write(new DataOutputStream(out));
			
			ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
			HBaseValue outputVal = new HBaseValue();
			outputVal.readFields(new DataInputStream(in));
			
			Value underlying = outputVal.getUnderlyingValue();
			
			assertTrue(underlying instanceof Literal);
				
			assertTrue(underlying.equals(inputLiteral));
			System.out.println(outputVal);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testLanguageLiteral(){
		try {
			Value inputLiteral = new LiteralImpl("This a Test Language Literal", new String("en"));
			HBaseValue inputVal = new HBaseValue(inputLiteral);
			
			ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
			inputVal.write(new DataOutputStream(out));
			
			ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
			HBaseValue outputVal = new HBaseValue();
			outputVal.readFields(new DataInputStream(in));
			
			Value underlying = outputVal.getUnderlyingValue();
			
			assertTrue(underlying instanceof Literal);
				
			assertTrue(underlying.equals(inputLiteral));
			System.out.println(outputVal);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
}
