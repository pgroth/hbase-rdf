package nl.vu.jena.sparql.engine.hsp;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;



public class MaximumIndependentSet {
	
	public static ArrayList<HashSet<WeightedGraphNode>> computeSets(WeightedGraph graph){
		
		graph.sortWeightDescending();
		WeightedGraph complement = graph.getComplementGraph();
		
		ArrayList<HashSet<WeightedGraphNode>> maximumSets = computeMaximumWeightCliques(complement);
		
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
			if (node.getWeight()==0){
				continue;
			}
			
			max = recursivelyComputeMaxClique(max, node.getNeighbors(), node.getWeight(), false,
						cliqueWeights, node, maximumCliques);
			cliqueWeights[i] = max;
		}
		
		/*System.out.println("Maximum weight "+max);
		for (int i = 0; i < cliqueWeights.length; i++) {
			System.out.print(cliqueWeights[i]+" ");
		}*/
		
		return maximumCliques;
	}
	
	/**
	 * Assuming the workingSet is sorted in descending order by weight
	 * 
	 * @param max - the maximum weight discovered so far
	 * @param workingSet - the sets of nodes which are considered in the current step
	 * @param weight - the weight accumulated so far
	 * @param hasNewNodes - indicates whether we are processing a clique of maximum weight
	 * @param cliqueWeights - the array of maximum weights by node
	 * @param current - the node from whose neighbors the working set is derived from
	 * @param maximumSets - the list of final solutions
	 * @return the new maximum
	 */
	private static int recursivelyComputeMaxClique(int max, List<WeightedGraphNode> workingSet, 
			int weight, boolean hasNewNodes,
			int []cliqueWeights,
			WeightedGraphNode current,
			ArrayList<HashSet<WeightedGraphNode>> maximumSets){
		
		if (workingSet.isEmpty()){
			if (weight>max){
				maximumSets.clear();
			}
			if (weight>max || (weight==max && hasNewNodes)){
				HashSet<WeightedGraphNode> newMaximumSet = new HashSet<WeightedGraphNode>();
				newMaximumSet.add(current);
				maximumSets.add(newMaximumSet);
				return weight;
			}
			else{
				return max;
			}
		}
		
		while (!workingSet.isEmpty()){
			boolean localHasNewNodes = false;
			WeightedGraphNode heaviest=null;
			
			int totalWeight = getTotalWeight(workingSet)+weight;
			//check pruning conditions
			if (totalWeight < max){
				return max;
			}
			else if (totalWeight == max && hasNewNodes==false){
				boolean exists = checkIfNodeExistsInMaximumSets(current, maximumSets);
				if (exists){
					return max;
				}
				else{
					heaviest = workingSet.remove(0);
					localHasNewNodes = true;
				}
			}
			else if (hasNewNodes==false){
				heaviest = workingSet.remove(0);
				if (cliqueWeights[heaviest.getSortedIndex()] + weight <= max) {
					return max;
				}
			}
			else{
				heaviest = workingSet.remove(0);
			}
			
			//build new working set
			List<WeightedGraphNode> newWorkingSet = new ArrayList<WeightedGraphNode>();
			newWorkingSet.addAll(heaviest.getNeighbors());
			newWorkingSet.retainAll(workingSet);
				
			int maximumSetsSizeBefore = maximumSets.size();
			int maxBefore = max;
			max = recursivelyComputeMaxClique(max, newWorkingSet, weight+heaviest.getWeight(), hasNewNodes ? hasNewNodes:localHasNewNodes,
					cliqueWeights, 
					heaviest, maximumSets);
			if (max==maxBefore){
				addCurrentNodeToSolution(current, maximumSets, maximumSetsSizeBefore);
			}
			else{//max has changed, just add it
				maximumSets.get(0).add(current);
			}
		}
		
		return max;
	}

	private static final void addCurrentNodeToSolution(WeightedGraphNode current, 
			ArrayList<HashSet<WeightedGraphNode>> maximumSets, int maximumSetsSize) {

		for (int i = maximumSetsSize; i < maximumSets.size(); i++) {
			maximumSets.get(i).add(current);
		}
	}

	private static boolean checkIfNodeExistsInMaximumSets(WeightedGraphNode current,
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


}
