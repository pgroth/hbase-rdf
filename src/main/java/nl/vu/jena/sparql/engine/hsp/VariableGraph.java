package nl.vu.jena.sparql.engine.hsp;

import java.util.HashMap;


import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.BasicPattern;

public class VariableGraph extends WeightedGraph{

	public VariableGraph(int size) {
		super(size);
	}
	
	public VariableGraph(BasicPattern pattern){
		super(pattern.size());
		
		HashMap<String, WeightedGraphNode> nodeMap = buildNodeMap(pattern); 
		
		for (WeightedGraphNode graphNode : nodeMap.values()) {
			super.addNode(graphNode);
		}
	}

	private HashMap<String, WeightedGraphNode> buildNodeMap(BasicPattern pattern) {
		HashMap<String, WeightedGraphNode> nodeMap = new HashMap<String, WeightedGraphNode>();
		for (Triple triple : pattern) {
			HashMap<String, WeightedGraphNode> tempMap = new HashMap<String, WeightedGraphNode>();
			addNewNode(nodeMap, tempMap, triple.getSubject());
			addNewNode(nodeMap, tempMap, triple.getPredicate());
			addNewNode(nodeMap, tempMap, triple.getObject());
			
			for (WeightedGraphNode node : tempMap.values()) {
				for (WeightedGraphNode node2 : tempMap.values()) {
					if (node!=node2){
						node.addNeighbor(node2);
					}
				}
			}
			
			nodeMap.putAll(tempMap);
		}
		return nodeMap;
	}

	private void addNewNode(HashMap<String, WeightedGraphNode> nodeMap, 
			HashMap<String, WeightedGraphNode> tempMap,
			Node node) {
		if (node.isVariable()){
			WeightedGraphNode existingNode = nodeMap.get(node.getName());
			if (existingNode==null && !tempMap.containsKey(node.getName())){
				tempMap.put(node.getName(), new WeightedGraphNode(node.getName(), 0));
			}
			else{
				existingNode.incWeight();
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
