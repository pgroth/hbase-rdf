package nl.vu.jena.sparql.engine.iterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WeightedGraphNode implements Comparable<WeightedGraphNode> {
	
	private int id;
	private int sortedIndex;
	private int weight;
	
	private List<WeightedGraphNode> neighbors;

	public WeightedGraphNode(int id, int weight) {
		super();
		this.id = id;
		this.weight = weight;
		this.neighbors = new ArrayList<WeightedGraphNode>();
	}

	public int getWeight() {
		return weight;
	}

	public List<WeightedGraphNode> getNeighbors() {
		return neighbors;
	}
	
	public void setNeighbors(List<WeightedGraphNode> neighbors) {
		this.neighbors = neighbors;
	}

	public void addNeighbor(WeightedGraphNode newNode){
		neighbors.add(newNode);
	}
	
	public int getId(){
		return id;
	}

	public int getSortedIndex() {
		return sortedIndex;
	}

	public void setSortedIndex(int sortedIndex) {
		this.sortedIndex = sortedIndex;
	}
	
	public void sortNeighborsWeightDescending(){
		Collections.sort(neighbors);
	}

	@Override
	public int compareTo(WeightedGraphNode o) {//weight descending order is natural
		if (weight>o.getWeight()){
			return -1;
		}
		else if (weight == o.getWeight()){
			return 0;
		}
		else{
			return 1;
		}
	}
	
	public String serializeNeighbors(){
		StringBuilder sb = new StringBuilder("{");
		if (neighbors.size()>0){
			sb.append(""+neighbors.get(0).getId());
			for (int i = 1; i < neighbors.size(); i++) {
				sb.append(", "+neighbors.get(i).getId());
			}
		}
		sb.append("}");
		return sb.toString();
	}
	
	public String toString(){
		String ret = new String("("+id+", "+weight+" ");
		ret += serializeNeighbors();
		ret += ")";
		return ret;
	}

	
}
