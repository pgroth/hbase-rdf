package nl.vu.jena.graph;

import java.io.IOException;
import java.util.ArrayList;

import nl.vu.datalayer.hbase.HBaseClientSolution;

import org.openjena.jenasesame.impl.Convert;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NullIterator;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

public class HBaseGraph extends GraphBase {

	private HBaseClientSolution hbase;
	ExtendedIterator<Triple> it;
	private ValueFactory valFactory;
	
	public HBaseGraph(HBaseClientSolution hbase) {
		super();
		this.hbase = hbase;
		valFactory = new ValueFactoryImpl();
	}

	@Override
	protected ExtendedIterator<Triple> graphBaseFind(TripleMatch m) {
		
		//convert TripleMatch to Value[]
		Node subject = m.getMatchSubject();
		Node predicate = m.getMatchPredicate();
		Node object =  m.getMatchObject();
		
		Value []quad = {Convert.nodeToValue(valFactory, subject), 
				Convert.nodeToValue(valFactory, predicate), 
				Convert.nodeToValue(valFactory, object), 
				null};
		
		//retrieve results from HBase
		ArrayList<ArrayList<Value>> results;
		try {
			results = hbase.util.getResults(quad);
		} catch (IOException e) {
			return NullIterator.instance();
		}
		
		//convert ArrayList<ArrayList<Value>> to ArrayList<Triple>
		ArrayList<Triple> convertedTriples = new ArrayList<Triple>(results.size());
		for (ArrayList<Value> arrayList : results) {
			Triple newTriple = new Triple(Convert.valueToNode(arrayList.get(0)),
									Convert.valueToNode(arrayList.get(1)),
									Convert.valueToNode(arrayList.get(2)));
			
			convertedTriples.add(newTriple);
		}	
		
		return WrappedIterator.createNoRemove(convertedTriples.iterator()) ;
	}

}
