package nl.vu.jena.sparql.engine.iterator;

import java.util.ArrayList;
import java.util.Collections;

public class WeightedGraph {

	private ArrayList<WeightedGraphNode> nodes;

	public WeightedGraph(int size) {
		super();
		this.nodes = new ArrayList<WeightedGraphNode>(size);
	}
	
	public void sortWeightDescending(){
		Collections.sort(nodes);
		for (int i = 0; i < nodes.size(); i++) {
			nodes.get(i).setSortedIndex(i);
			nodes.get(i).sortNeighborsWeightDescending();
		}
	}
	
	public WeightedGraphNode getNode(int i){
		if (i<0 || i>nodes.size()){
			throw new ArrayIndexOutOfBoundsException("Node index out of bounds");
		}
		return nodes.get(i);
	}
	
	public void addNode(WeightedGraphNode newNode){
		nodes.add(newNode);
	}
	
	public int getSize(){
		return nodes.size();
	}
	
	public ArrayList<WeightedGraphNode> getNodes() {
		return nodes;
	}

	public WeightedGraph getComplementGraph(){
		WeightedGraph complement = new WeightedGraph(nodes.size());
		
		
		for (WeightedGraphNode node : nodes) {
			WeightedGraphNode newNode = new WeightedGraphNode(node.getId(), node.getWeight());
			
			ArrayList<WeightedGraphNode> otherNeighbours = (ArrayList<WeightedGraphNode>)nodes.clone();
			otherNeighbours.removeAll(node.getNeighbors());
			otherNeighbours.remove(node);
			
			newNode.setNeighbors(otherNeighbours);
			complement.addNode(newNode);
		}
		
		return complement;
	}

	
	public String toString(){
		String ret = "";
		for (WeightedGraphNode node : nodes) {
			ret += node.toString()+" ; ";
		}
		
		return ret;
	}
	
	public static void main(String[] args) {
		WeightedGraphNode n1=null, n2=null, n3=null, n4, n5;
		n1 = new WeightedGraphNode(0, 8);
		n2 = new WeightedGraphNode(1, 5);
		n3 = new WeightedGraphNode(2, 6);
		n4 = new WeightedGraphNode(3, 9);
		n5 = new WeightedGraphNode(4, 10);
		
		n1.addNeighbor(n3);
		n1.addNeighbor(n5);
		
		n2.addNeighbor(n4);
		n2.addNeighbor(n5);
		
		n3.addNeighbor(n1);
		n3.addNeighbor(n5);
		
		n4.addNeighbor(n2);
		n4.addNeighbor(n5);
		
		n5.addNeighbor(n1);
		n5.addNeighbor(n2);
		n5.addNeighbor(n3);
		n5.addNeighbor(n4);
		
		WeightedGraph graph = new WeightedGraph(5);
		graph.addNode(n1);
		graph.addNode(n2);
		graph.addNode(n3);
		graph.addNode(n4);
		graph.addNode(n5);
		
		MaximumIndependentSet.getMaximumIndependentSets(graph);
	}
	
	
	
}
