package nl.vu.jena.graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.id.Id;
import nl.vu.datalayer.hbase.retrieve.IHBasePrefixMatchRetrieveOpsManager;
import nl.vu.datalayer.hbase.retrieve.RowLimitPair;
import nl.vu.jena.cache.JenaCache;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_Literal;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NullIterator;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

public class HBaseGraph extends GraphBase {

	private static final int CACHE_SIZE = 500;
	private HBaseClientSolution hbase;
	ExtendedIterator<Triple> it;
	ValueIdMapper valIdMapper;
	
	private Map<TripleMatch, ExtendedIterator<Triple>> cache = Collections.synchronizedMap(new JenaCache<TripleMatch, ExtendedIterator<Triple>>(CACHE_SIZE));
	
	public HBaseGraph(HBaseClientSolution hbase) {
		super();
		this.hbase = hbase;
		valIdMapper = new ValueIdMapper(hbase);
	}

	@Override
	protected ExtendedIterator<Triple> graphBaseFind(TripleMatch m) {	
		ExtendedIterator<Triple> ret;
		if ((ret=cache.get(m))!=null){
			return ret;
		}
		
		try {
			//TODO should try to get only Ids here, in order to minimize the number of lookups
			Id[] quad = valIdMapper.getIdsFromTriple(m);

			// retrieve results from HBase
			ArrayList<ArrayList<Id>> results;

			if (m instanceof FilteredTriple) {
				ExprFunction2 simpleFilter = (ExprFunction2) (((FilteredTriple) m)
						.getSimpleFilter());
				results = getFilteredResults(quad, simpleFilter);
			} else {
				results = ((IHBasePrefixMatchRetrieveOpsManager) hbase.opsManager).getResults(quad);
			}

		
			ArrayList<Triple> convertedTriples = new ArrayList<Triple>(results.size());
			for (ArrayList<Id> arrayList : results) {
				Triple newTriple = new Triple(Node.createUncachedLiteral(
						arrayList.get(0), null), Node.createUncachedLiteral(
						arrayList.get(1), null), Node.createUncachedLiteral(
						arrayList.get(2), null));

				convertedTriples.add(newTriple);
			}

			ret = WrappedIterator.createNoRemove(convertedTriples.iterator());
			cache.put(m, ret);

		} catch (Exception e) {
			e.printStackTrace();
			return NullIterator.instance();
		}
		return ret;
	}
	
	public void mapNodeIdsToMaterializedNodes(Map<Node_Literal, Node> tempIdMap) throws IOException{
		valIdMapper.mapNodeIdsToMaterializedNodes(tempIdMap);
	}
	
	public void mapMaterializedNodesToNodeIds(Map<Node, Node_Literal> node2nodeIdMap) throws IOException{
		valIdMapper.mapMaterializedNodesToNodeIds(node2nodeIdMap);
	}

	private ArrayList<ArrayList<Id>> getFilteredResults(Id[] quad, ExprFunction2 simpleFilter)
			throws Exception, IOException {
		ArrayList<ArrayList<Id>> results;
		
		Expr arg1 = simpleFilter.getArg1();
		Expr arg2 = simpleFilter.getArg2();
		
		if (arg1.isConstant() && arg1.getConstant().isNumber() ||
				arg2.isConstant() && arg2.getConstant().isNumber()){
			RowLimitPair limitPair = ExprToHBaseLimitsConverter.getRowLimitPair(simpleFilter);
			results = ((IHBasePrefixMatchRetrieveOpsManager)hbase.opsManager).getResults(quad, limitPair);
		}
		/*TODO else if (simpleFilter instanceof E_Equals && 
				((arg1.isConstant()&&arg1.getConstant().isIRI()) 
				|| (arg2.isConstant()&&arg2.getConstant().isIRI()))){
			E_Equals eq = (E_Equals) simpleFilter;
			NodeValue constantNode = null;
			if (eq.getArg1().isConstant()) {
				constantNode = eq.getArg1().getConstant();
			} else if (eq.getArg2().isConstant()) {
				constantNode = eq.getArg2().getConstant();
			}
			
			Value constValue = Convert.nodeToValue(valFactory, constantNode.asNode());
			TODO have to convert equality Filters to Ids before getting here
			quad[2] = 
			results = hbase.opsManager.getResults(quad);
		}*/
		else 
			throw new RuntimeException("Unsupported simple filter: "+simpleFilter.getOpName());//TODO
		return results;
	}

}
