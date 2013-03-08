package nl.vu.jena.graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.id.Id;
import nl.vu.datalayer.hbase.operations.HBPrefixMatchOperationManager;
import nl.vu.datalayer.hbase.retrieve.HBaseTripleElement;
import nl.vu.datalayer.hbase.retrieve.IHBasePrefixMatchRetrieveOpsManager;
import nl.vu.datalayer.hbase.retrieve.RowLimitPair;
import nl.vu.jena.cache.JenaCache;

import org.openjena.jenasesame.impl.Convert;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_Literal;
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
		//ASSUMPTION: only NodeIds are received in this function
		
		ExtendedIterator<Triple> ret;
		if ((ret=cache.get(m))!=null){
			return ret;
		}
		
		//convert TripleMatch to Value[]
		Node subject = m.getMatchSubject();
		Node predicate = m.getMatchPredicate();
		Node object =  m.getMatchObject();
	
		Id []quad = {(subject!=null && subject.isConcrete()) ? (Id)subject.getLiteralValue():null, 
				(predicate!=null &&predicate.isConcrete()) ? (Id)predicate.getLiteralValue():null, 
				(object!=null && object.isConcrete()) ? (Id)object.getLiteralValue():null, 
				null};
		
		//retrieve results from HBase
		ArrayList<ArrayList<Id>> results;
		try {
			if (m instanceof FilteredTriple){
				ExprFunction2 simpleFilter = (ExprFunction2)(((FilteredTriple)m).getSimpleFilter());
				results = getFilteredResults(quad, simpleFilter);
			}
			else{
				results = ((IHBasePrefixMatchRetrieveOpsManager)hbase.opsManager).getResults(quad);
			}
		} catch (Exception e) {
			return NullIterator.instance();
		}
		
		//convert ArrayList<ArrayList<Value>> to ArrayList<Triple>
		ArrayList<Triple> convertedTriples = new ArrayList<Triple>(results.size());
		for (ArrayList<Id> arrayList : results) {
			Triple newTriple = new Triple(Node.createUncachedLiteral(arrayList.get(0), null),
									Node.createUncachedLiteral(arrayList.get(1), null),
									Node.createUncachedLiteral(arrayList.get(2), null));
			
			convertedTriples.add(newTriple);
		}	
		
		ret = WrappedIterator.createNoRemove(convertedTriples.iterator()) ;
		cache.put(m, ret);
		
		return ret;
	}
	
	public void mapNodeIdsToMaterializedNodes(Map<Node_Literal, Node> tempIdMap) throws IOException{
		Map<Id, Value> toResolve = new HashMap<Id, Value>(tempIdMap.size());
		for (Map.Entry<Node_Literal, Node> entry : tempIdMap.entrySet()) {
			toResolve.put((Id)entry.getKey().getLiteralValue(), null);
		}
		
		((IHBasePrefixMatchRetrieveOpsManager)hbase.opsManager).materializeIds(toResolve);
		
		for (Map.Entry<Node_Literal, Node> entry : tempIdMap.entrySet()) {
			Id id = (Id)entry.getKey().getLiteralValue();
			Node newNode = Convert.valueToNode(toResolve.get(id));
			entry.setValue(newNode);
		}
	}
	
	public void mapMaterializedNodesToNodeIds(Map<Node, Node_Literal> node2nodeIdMap) throws IOException{
		Map<Value, Id> toResolve = new HashMap<Value, Id>(node2nodeIdMap.size());
		Map<Node, Value> tempMapping = new HashMap<Node, Value>(node2nodeIdMap.size());
		
		for (Map.Entry<Node, Node_Literal> mapEntry : node2nodeIdMap.entrySet()) {
			Value value = Convert.nodeToValue(valFactory, mapEntry.getKey());
			tempMapping.put(mapEntry.getKey(), value);
			toResolve.put(value, null);
		}
		
		((IHBasePrefixMatchRetrieveOpsManager)hbase.opsManager).mapValuesToIds(toResolve);
		
		for (Map.Entry<Node, Node_Literal> mapEntry : node2nodeIdMap.entrySet()) {
			Value toUpdate = tempMapping.get(mapEntry.getKey());
			Id id = toResolve.get(toUpdate);
			mapEntry.setValue((Node_Literal)Node.createUncachedLiteral(id, null));
		}
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
