package nl.vu.jena.graph;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.id.Id;
import nl.vu.datalayer.hbase.operations.prefixmatch.IHBasePrefixMatchRetrieveOpsManager;
import nl.vu.datalayer.hbase.parameters.Quad;

import org.openjena.jenasesame.impl.Convert;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_Literal;
import com.hp.hpl.jena.graph.TripleMatch;

public class ValueIdMapper {
	
	private ValueFactory valFactory;
	
	private HBaseClientSolution hbase;

	public ValueIdMapper(HBaseClientSolution hbase) {
		this.hbase = hbase;
		valFactory = new ValueFactoryImpl();
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
			if (id != null){
				mapEntry.setValue((Node_Literal)Node.createUncachedLiteral(id, null));
			}
		}
	}
	
	public Quad getIdsFromTriple(TripleMatch m) throws IOException{
		Node subject = m.getMatchSubject();
		Node predicate = m.getMatchPredicate();
		Node object =  m.getMatchObject();
		
		Map<Value, Id> toResolve = new HashMap<Value, Id>();
		checkNode(toResolve, subject);
		checkNode(toResolve, predicate);
		checkNode(toResolve, object);
		
		if (!toResolve.isEmpty()){
			((IHBasePrefixMatchRetrieveOpsManager)hbase.opsManager).mapValuesToIds(toResolve);
		}
		
		Id[] retIds = { addNodeToMap(toResolve, subject),
	             addNodeToMap(toResolve, predicate),
	             addNodeToMap(toResolve, object),
	             null};
			
		return new Quad(retIds);
	}
	
	private void checkNode(Map<Value, Id> toResolve, Node node){
		if (node == null || !node.isConcrete())
			return;
		
		if (!(node instanceof Node_Literal && node.getLiteralValue() instanceof Id)){
			Value val = Convert.nodeToValue(valFactory, node);
			toResolve.put(val, null);
		}
	}
	
	private Id addNodeToMap(Map<Value, Id> resolved, Node node) {
		if (node == null || !node.isConcrete())
			return null;
		
		if (node instanceof Node_Literal && node.getLiteralValue() instanceof Id){
			return (Id)node.getLiteralValue();
		}
		else {
			Value val = Convert.nodeToValue(valFactory, node);
			return resolved.get(val);
		}
	}

}
