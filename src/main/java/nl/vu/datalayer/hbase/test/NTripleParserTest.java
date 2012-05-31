package nl.vu.datalayer.hbase.test;

import nl.vu.datalayer.hbase.id.TypedId;

import org.apache.hadoop.io.Text;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.ntriples.NTriplesUtil;

public class NTripleParserTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try{
			ValueFactory valFact = new ValueFactoryImpl();
			//Value object = NTriplesUtil.parseValue("<http://xmlns.com/foaf/0.1/Person>", valFact);
			//Value object = NTriplesUtil.parseValue("_:genid1", valFact);
			Value object = NTriplesUtil.parseValue("\"2423\"^^<http://www.w3.org/2001/XMLSchema#integer>", valFact);
			//Value object = NTriplesUtil.parseValue("\"8082139320607108363\"^^<http://www.w3.org/2001/XMLSchema#positiveInteger>", valFact);
			//Value object = NTriplesUtil.parseValue("\"-71.091840\"^^<http://www.w3.org/2001/XMLSchema#double>", valFact);
			//Value object = NTriplesUtil.parseValue("\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>", valFact);
			//Value object = NTriplesUtil.parseValue("\"-3591\"^^<http://www.w3.org/2001/XMLSchema#int>", valFact);
			//Value object = NTriplesUtil.parseValue("\"4294967295\"^^<http://www.w3.org/2001/XMLSchema#unsignedInt>", valFact);
			//String objectElem = new String("\"-71.091840\"^^<http://www.w3.org/2001/XMLSchema#double>");
			//Text t = new Text(objectElem);
			//Value object = NTriplesUtil.parseValue(t.toString(), valFact);
			//Value object = NTriplesUtil.parseValue("\"2385646\"^^<http://www.w3.org/2001/XMLSchema#integer>", valFact);
			//Value object = NTriplesUtil.parseValue("\"-8583978581523500142\"^^<http://www.w3.org/2001/XMLSchema#negativeInteger>", valFact);
			//Value object = NTriplesUtil.parseValue("\"575791.868\"^^<http://www.w3.org/2001/XMLSchema#decimal>", valFact);
			//Value object = NTriplesUtil.parseValue("\"256\"^^<http://www.w3.org/2001/XMLSchema#unsignedByte>", valFact);
			//Value object = NTriplesUtil.parseValue("\"18446744073709551615\"^^<http://www.w3.org/2001/XMLSchema#unsignedLong>", valFact);
			

			if (object instanceof URI) {
				URI uri = (URI) object;
				System.out.println("Namespace: " + uri.getNamespace() + "; LocalName: " + uri.getLocalName());
			} else if (object instanceof Literal) {
				Literal literal = (Literal) object;
				System.out.println("Language: " + literal.getLanguage() + "; Label: " + literal.getLabel() + "; URI " + literal.getDatatype());

				TypedId id = TypedId.createNumerical(literal);
				if (id == null) {
					System.out.println("Not a number");
				} else {
					if (id.getType() != TypedId.NUMERICAL) {
						System.out.println("Wrong type");
					}

					System.out.println("Number");
					for (int i = 0; i < id.getBytes().length; i++) {
						System.out.print(String.format("%x ", id.getBytes()[i]));
					}
					System.out.println();

					System.out.println(id);
					
					Literal l = id.toLiteral();
					
					System.out.println(l);
				}
			} else if (object instanceof BNode) {
				BNode node = (BNode)object;
				System.out.println("BNodeId: " + node.getID() + "; String value: " + node.stringValue());
			}

		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
