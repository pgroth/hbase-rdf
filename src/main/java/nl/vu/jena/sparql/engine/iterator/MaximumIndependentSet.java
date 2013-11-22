package nl.vu.jena.sparql.engine.iterator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;


public class MaximumIndependentSet {
	
	public static ArrayList<HashSet<WeightedGraphNode>> getMaximumIndependentSets(WeightedGraph graph){
		
		graph.sortWeightDescending();
		//WeightedGraph complement = graph.getComplementGraph();
		
		ArrayList<HashSet<WeightedGraphNode>> maximumSets = computeMaximumWeightCliques(graph);
		
		return maximumSets;
	}

	private static ArrayList<HashSet<WeightedGraphNode>> computeMaximumWeightCliques(WeightedGraph complement) {
		
		ArrayList<HashSet<WeightedGraphNode>> maximumCliques = new ArrayList<HashSet<WeightedGraphNode>>();
		
		int max = 0;
		
		int []cliqueWeights = new int[complement.getSize()];
		for (int i = 0; i < cliqueWeights.length; i++) {
			cliqueWeights[i] = 0;
		}
		
		for (int i = cliqueWeights.length-1; i>=0; i--) {
			WeightedGraphNode node = complement.getNode(i);
			max = recursivelyComputeMaxClique(max, cliqueWeights, node.getNeighbors(), node.getWeight(),
						false, node, maximumCliques);
			cliqueWeights[i] = max;
		}
		
		System.out.println("Maximum weight "+max);
		for (int i = 0; i < cliqueWeights.length; i++) {
			System.out.print(cliqueWeights[i]+" ");
		}
		
		return maximumCliques;
	}
	
	/**
	 * Assuming the workingSet is sorted in descending order by weight
	 * 
	 * @param max
	 * @param cliqueWeights
	 * @param workingSet
	 * @param weight
	 * @return
	 */
	private static int recursivelyComputeMaxClique(int max, int []cliqueWeights, 
			List<WeightedGraphNode> workingSet, int weight,
			boolean hasNewNodes,
			WeightedGraphNode current,
			ArrayList<HashSet<WeightedGraphNode>> maximumSets){
		
		if (workingSet.isEmpty()){
			if (weight>max || (weight==max && hasNewNodes)){
				HashSet<WeightedGraphNode> newMaximumSet = new HashSet<WeightedGraphNode>();
				newMaximumSet.add(current);
				maximumSets.add(0, newMaximumSet);
				return weight;
			}
			else{
				return max;
			}
		}
		
		int maximumSetsSize = maximumSets.size();
		
		while (!workingSet.isEmpty()){
			boolean localHasNewNodes = false;
			WeightedGraphNode first=null;
			
			int totalWeight = getTotalWeight(workingSet)+weight;
			if (totalWeight < max){
				addCurrentNodeToSolution(current, maximumSets, maximumSetsSize);
				return max;
			}
			else if (totalWeight == max && hasNewNodes==false){
				boolean exists = checkMaximumSets(current, maximumSets);
				if (exists){
					addCurrentNodeToSolution(current, maximumSets, maximumSetsSize);
					return max;
				}
				else{
					first = workingSet.remove(0);
					localHasNewNodes = true;
				}
			}
			else if (hasNewNodes==false){
				first = workingSet.remove(0);
				if (cliqueWeights[first.getSortedIndex()] + weight <= max) {
					addCurrentNodeToSolution(current, maximumSets,
							maximumSetsSize);
					return max;
				}
			}
			else{
				first = workingSet.remove(0);
			}
			
			List<WeightedGraphNode> newWorkingSet = new ArrayList<WeightedGraphNode>();
			newWorkingSet.addAll(first.getNeighbors());
			newWorkingSet.retainAll(workingSet);
				
			max = recursivelyComputeMaxClique(max, cliqueWeights, newWorkingSet, weight+first.getWeight(),
					hasNewNodes ? hasNewNodes:localHasNewNodes, 
							first, maximumSets);
		}
		
		addCurrentNodeToSolution(current, maximumSets, maximumSetsSize);
		
		return max;
	}

	private static final void addCurrentNodeToSolution(WeightedGraphNode current,
			ArrayList<HashSet<WeightedGraphNode>> maximumSets,
			int maximumSetsSize) {
		if (maximumSetsSize!=maximumSets.size()){//add the last element
			maximumSets.get(0).add(current);
		}
	}

	private static boolean checkMaximumSets(WeightedGraphNode current,
			ArrayList<HashSet<WeightedGraphNode>> maximumSets) {
		boolean exists = false;
		for (HashSet<WeightedGraphNode> maximumSet : maximumSets) {
			if (maximumSet.contains(current)){
				exists=true;
				break;
			}
		}
		return exists;
	}
	
	private static int getTotalWeight(List<WeightedGraphNode> nodes){
		int sum=0;
		for (WeightedGraphNode node : nodes) {
			sum+=node.getWeight();
		}
		
		return sum;
	}

	public static void main(String[] args) {
		
	}
}
