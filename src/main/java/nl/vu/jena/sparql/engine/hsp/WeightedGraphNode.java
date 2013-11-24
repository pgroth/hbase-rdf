package nl.vu.jena.sparql.engine.hsp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WeightedGraphNode implements Comparable<WeightedGraphNode> {
	
	private String id;
	private int sortedIndex;
	private int weight;
	
	private List<WeightedGraphNode> neighbors;

	public WeightedGraphNode(String id, int weight) {
		super();
		this.id = id;
		this.weight = weight;
		this.neighbors = new ArrayList<WeightedGraphNode>();
	}

	public int getWeight() {
		return weight;
	}
	
	public void incWeight(){
		weight++;
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
	
	public String getId(){
		return id;
	}

	public void setId(String id) {
		this.id = id;
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
	
	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof WeightedGraphNode)){
			throw new RuntimeException("Equals applied on WeightedGraphNode and "+obj.getClass().getName());
		}
		WeightedGraphNode other = (WeightedGraphNode)obj;
		return id.equals(other.getId());
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
