package nl.vu.datalayer.hbase.id;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import nl.vu.datalayer.hbase.exceptions.NumericalRangeException;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.ntriples.NTriplesUtil;

public class TypedIdTestNumericals {

	private ValueFactory valFact;
	
	@Before
	public void runBeforeEveryTest() {
		 valFact = new ValueFactoryImpl();
	}
	
	//================================= XSD_Integer Numericals ==========================
	
	@Test
	public void test_integer_PositiveInput() {	
		String sNumber = "2423";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#integer>";
			testNumerical(toTest);		
		} catch (NumericalRangeException e) {
			e.printStackTrace();
			fail("NumericalRangeException thrown for "+sNumber);
		}
	}
	
	@Test
	public void test_integer_ZeroInput() {	
		String sNumber = "0";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#integer>";
			testNumerical(toTest);		
		} catch (NumericalRangeException e) {
			e.printStackTrace();
			fail("NumericalRangeException thrown for "+sNumber);
		}
	}
	
	@Test
	public void test_integer_NegativeInput() {	
		String sNumber = "-2423";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#integer>";
			testNumerical(toTest);	
		} catch (NumericalRangeException e) {
			e.printStackTrace();
			fail("NumericalRangeException thrown for "+sNumber);
		}
	}
	
	@Test
	public void test_integer_FormatException() {	
		String sNumber = "2423.4";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#integer>";
			testNumerical(toTest);		
		} catch (NumericalRangeException e) {
			e.printStackTrace();
			fail("NumericalRangeException thrown for "+sNumber);
		}
		catch (NumberFormatException e) {
			//let the test pass
		}
	}
	
	//================================= XSD_PositiveInteger Numericals ==========================
	
	@Test
	public void test_positiveInteger_NegativeInput() {	
		String sNumber = "-2423";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#positiveInteger>";
			Value object = NTriplesUtil.parseValue(toTest, valFact);
			TypedId id = TypedId.createNumerical((Literal)object);
			
			fail("NumericalRangeException not thrown for positiveInteger "+sNumber);
		} catch (NumericalRangeException e) {
			//let the test pass
		}
	}
	
	@Test
	public void test_positiveInteger_PositiveInput() {	
		String sNumber = "123344323";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#positiveInteger>";
			testNumerical(toTest);
		} catch (NumericalRangeException e) {
			e.printStackTrace();
			fail("NumericalRangeException thrown for "+sNumber);
		}
	}
	
	@Test
	public void test_positiveInteger_ZeroInput() {	
		String sNumber = "0";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#positiveInteger>";
			Value object = NTriplesUtil.parseValue(toTest, valFact);
			TypedId id = TypedId.createNumerical((Literal)object);
			
			fail("NumericalRangeException not thrown for positiveInteger "+sNumber);
		} catch (NumericalRangeException e) {
			//let the test pass
		}
	}
	
	//================================= XSD_Double Numericals ==========================
	
	@Test
	public void test_double() {	
		String sNumber = "123323.234445";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#double>";
			testNumerical(toTest);
		} catch (NumericalRangeException e) {
			e.printStackTrace();
			fail("NumericalRangeException thrown for "+sNumber);
		}
	}
	
	@Test
	public void test_float() {	//floats are converted to doubles
		String sNumber = "123323.234445";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#float>";
			Value object = NTriplesUtil.parseValue(toTest, valFact);
			TypedId id = TypedId.createNumerical((Literal)object);
			assertTrue(id != null);
			
			Literal l = id.toLiteral();
			assertTrue(l.toString().equals("\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#double>"));
		} catch (NumericalRangeException e) {
			e.printStackTrace();
			fail("NumericalRangeException thrown for "+sNumber);
		}
	}
	
	//================================= XSD_int Numericals ==========================
	
	@Test
	public void test_int() {	
		String sNumber = "12323";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#int>";
			testNumerical(toTest);
		} catch (NumericalRangeException e) {
			e.printStackTrace();
			fail("NumericalRangeException thrown for "+sNumber);
		}
	}
	
	@Test
	public void test_int_FormatException() {	
		String sNumber = "12323.456";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#int>";
			Value object = NTriplesUtil.parseValue(toTest, valFact);
			TypedId id = TypedId.createNumerical((Literal)object);
			
			fail("NumberFormatException not thrown for int "+sNumber);
		} 
		catch (NumericalRangeException e) {
			fail("NumericalRangeException thrown for "+sNumber);
		}
		catch (NumberFormatException e) {
			//let the test pass
		}
	}
	
	@Test
	public void test_int_AboveMax() {	
		String sNumber = "2147483648";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#int>";
			Value object = NTriplesUtil.parseValue(toTest, valFact);
			TypedId id = TypedId.createNumerical((Literal)object);
			
			fail("NumberFormatException not thrown for int "+sNumber);
		}
		catch (NumericalRangeException e) {
			fail("NumericalRangeException thrown for "+sNumber);
		}
		catch (NumberFormatException e) {//this will be thrown instead of NumericalRangeException
										 //because it comes from Integer.parseInt
			//let the test pass	
		}
	}
	
	//================================= XSD_unsignedInt Numericals ==========================
	
	@Test
	public void test_unsignedInt_positiveInput() {	
		String sNumber = "12323";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#unsignedInt>";
			testNumerical(toTest);
		} catch (NumericalRangeException e) {
			e.printStackTrace();
			fail("NumericalRangeException thrown for "+sNumber);
		}
	}
	
	@Test
	public void test_unsignedInt_negativeInput() {	
		String sNumber = "-12323";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#unsignedInt>";
			Value object = NTriplesUtil.parseValue(toTest, valFact);
			TypedId id = TypedId.createNumerical((Literal)object);
			
			fail("NumericalRangeException not thrown for unsignedInt "+sNumber);
		} catch (NumericalRangeException e) {
			//let the test pass
		}
	}
	
	@Test
	public void test_unsignedInt_zeroInput() {	
		String sNumber = "0";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#unsignedInt>";
			testNumerical(toTest);
		} catch (NumericalRangeException e) {
			e.printStackTrace();
			fail("NumericalRangeException thrown for "+sNumber);
		}
	}
	
	@Test
	public void test_unsignedInt_aboveMax() {	
		String sNumber = "4294967296";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#unsignedInt>";
			Value object = NTriplesUtil.parseValue(toTest, valFact);
			TypedId id = TypedId.createNumerical((Literal)object);
			
			fail("NumericalRangeException not thrown for unsignedInt "+sNumber);
		} catch (NumericalRangeException e) {
			//let the test pass
		}
	}
	
	//================================= XSD_unsignedShort Numericals ==========================
	
	@Test
	public void test_unsignedShort_positiveInput() {	
		String sNumber = "12323";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#unsignedShort>";
			testNumerical(toTest);
		} catch (NumericalRangeException e) {
			e.printStackTrace();
			fail("NumericalRangeException thrown for "+sNumber);
		}
	}
	
	@Test
	public void test_unsignedShort_negativeInput() {	
		String sNumber = "-1323";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#unsignedShort>";
			Value object = NTriplesUtil.parseValue(toTest, valFact);
			TypedId id = TypedId.createNumerical((Literal)object);
			
			fail("NumericalRangeException not thrown for unsignedInt "+sNumber);
		} catch (NumericalRangeException e) {
			//let the test pass
		}
	}
	
	@Test
	public void test_unsignedShort_aboveMax() {	
		String sNumber = "65536";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#unsignedShort>";
			Value object = NTriplesUtil.parseValue(toTest, valFact);
			TypedId id = TypedId.createNumerical((Literal)object);
			
			fail("NumericalRangeException not thrown for unsignedInt "+sNumber);
		} catch (NumericalRangeException e) {
			//let the test pass
		}
	}
	
	//================================= XSD_unsignedByte Numericals ==========================
	
	@Test
	public void test_unsignedByte_positiveInput() {	
		String sNumber = "168";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#unsignedByte>";
			testNumerical(toTest);
		} catch (NumericalRangeException e) {
			e.printStackTrace();
			fail("NumericalRangeException thrown for "+sNumber);
		}
	}
	
	@Test
	public void test_unsignedByte_negativeInput() {	
		String sNumber = "-132";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#unsignedByte>";
			Value object = NTriplesUtil.parseValue(toTest, valFact);
			TypedId id = TypedId.createNumerical((Literal)object);
			
			fail("NumericalRangeException not thrown for unsignedInt "+sNumber);
		} catch (NumericalRangeException e) {
			//let the test pass
		}
	}
	
	@Test
	public void test_unsignedByte_aboveMax() {	
		String sNumber = "256";
		try {
			String toTest = "\""+sNumber+"\"^^<http://www.w3.org/2001/XMLSchema#unsignedByte>";
			Value object = NTriplesUtil.parseValue(toTest, valFact);
			TypedId id = TypedId.createNumerical((Literal)object);
			
			fail("NumericalRangeException not thrown for unsignedInt "+sNumber);
		} catch (NumericalRangeException e) {
			//let the test pass
		}
	}

	@Ignore
	private void testNumerical(String toTest) throws NumericalRangeException{
		Value object = NTriplesUtil.parseValue(toTest, valFact);
		TypedId id = TypedId.createNumerical((Literal)object);
		assertTrue(id != null);
		
		Literal l = id.toLiteral();
		assertTrue(l.toString().equals(toTest));
	}
}
