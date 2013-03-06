package nl.vu.jena.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.operations.HBPrefixMatchOperations;
import nl.vu.datalayer.hbase.retrieve.HBaseGeneric;
import nl.vu.datalayer.hbase.retrieve.IHBasePrefixMatchRetrieve;
import nl.vu.datalayer.hbase.retrieve.RowLimitPair;
import nl.vu.jena.cache.JenaCache;

import org.openjena.jenasesame.impl.Convert;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NullIterator;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

public class HBaseGraph extends GraphBase {

	private static final int CACHE_SIZE = 500;
	private HBaseClientSolution hbase;
	ExtendedIterator<Triple> it;
	private ValueFactory valFactory;
	
	private Map<TripleMatch, ExtendedIterator<Triple>> cache = Collections.synchronizedMap(new JenaCache<TripleMatch, ExtendedIterator<Triple>>(CACHE_SIZE));
	
	public HBaseGraph(HBaseClientSolution hbase) {
		super();
		this.hbase = hbase;
		valFactory = new ValueFactoryImpl();
	}

	@Override
	protected ExtendedIterator<Triple> graphBaseFind(TripleMatch m) {
		
		ExtendedIterator<Triple> ret;
		if ((ret=cache.get(m))!=null){
			return ret;
		}
		
		//convert TripleMatch to Value[]
		Node subject = m.getMatchSubject();
		Node predicate = m.getMatchPredicate();
		Node object =  m.getMatchObject();
	
		Value []quad = {(subject!=null && subject.isConcrete()) ? Convert.nodeToValue(valFactory, subject):null, 
				(predicate!=null &&predicate.isConcrete()) ? Convert.nodeToValue(valFactory, predicate):null, 
				(object!=null && object.isConcrete()) ? Convert.nodeToValue(valFactory, object):null, 
				null};
		
		//retrieve results from HBase
		ArrayList<ArrayList<Value>> results;
		try {
			if (m instanceof FilteredTriple){
				ExprFunction2 simpleFilter = (ExprFunction2)(((FilteredTriple)m).getSimpleFilter());
				Expr arg1 = simpleFilter.getArg1();
				Expr arg2 = simpleFilter.getArg2();
				if (arg1.isConstant() && arg1.getConstant().isNumber() ||
						arg2.isConstant() && arg2.getConstant().isNumber()){
					RowLimitPair limitPair = ExprToHBaseLimitsConverter.getRowLimitPair(simpleFilter);
					HBaseGeneric []genericPattern = ((HBPrefixMatchOperations)hbase.util).convertQuadToGenericPattern(quad);
					results = ((IHBasePrefixMatchRetrieve)hbase.util).getMaterializedResults(genericPattern, limitPair);
				}
				else if (simpleFilter instanceof E_Equals && 
						((arg1.isConstant()&&arg1.getConstant().isIRI()) 
						|| (arg2.isConstant()&&arg2.getConstant().isIRI()))){
					E_Equals eq = (E_Equals) simpleFilter;
					NodeValue constantNode = null;
					if (eq.getArg1().isConstant()) {
						constantNode = eq.getArg1().getConstant();
					} else if (eq.getArg2().isConstant()) {
						constantNode = eq.getArg2().getConstant();
					}
					
					quad[2] = Convert.nodeToValue(valFactory, constantNode.asNode());
					results = hbase.util.getResults(quad);
				}
				else 
					throw new RuntimeException("Unsupported simple filter: "+simpleFilter.getOpName());//TODO
			}
			else{
				results = hbase.util.getResults(quad);
			}
		} catch (Exception e) {
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
		
		ret = WrappedIterator.createNoRemove(convertedTriples.iterator()) ;
		cache.put(m, ret);
		
		return ret;
	}

}
