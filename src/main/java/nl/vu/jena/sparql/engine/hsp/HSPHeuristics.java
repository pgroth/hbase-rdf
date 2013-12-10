package nl.vu.jena.sparql.engine.hsp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.Vector;

import nl.vu.jena.sparql.engine.main.HBaseSymbols;

import org.apache.hadoop.hbase.util.Pair;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;

public class HSPHeuristics {

	public static final int S = 0x04;
	public static final int P = 0x02;
	public static final int O = 0x01;
	//public static final int SP = S|P;
	//public static final int SO = S|O;
	//public static final int PO = P|O;
	//lower weight means less selective, higher weight means more selective
	public static final int []joinPositionWeights = {0, 3/*O-O*/, 1/*P-P*/, 6/*P-O*/, 2/*S-S*/, 4/*S-O*/, 5/*S-P*/, 7/*S-P-O*/};

	/**
	 * Choose the patterns which have the most number of constants (BNodes, URIs or Literals)
	 * 
	 * @param pattern
	 * @param maximumISets
	 * @return
	 */
	static ArrayList<HashSet<WeightedGraphNode>> applyHeuristicH1H2(BasicPattern pattern, ArrayList<HashSet<WeightedGraphNode>> maximumISets, boolean onlyLiterals) {
		int maxConcrete = 0;
		ArrayList<HashSet<WeightedGraphNode>> newMaxISets = new ArrayList<HashSet<WeightedGraphNode>>();
		for (HashSet<WeightedGraphNode> maxISet : maximumISets) {
			
			int countConcrete = countConcrete(pattern, maxISet, onlyLiterals);
			
			if (countConcrete>maxConcrete){
				maxConcrete = countConcrete;
				newMaxISets.clear();
				newMaxISets.add(maxISet);
			}
			else if (countConcrete==maxConcrete){
				newMaxISets.add(maxISet);
			}
		}
		return newMaxISets;
	}

	private static int countConcrete(BasicPattern pattern, HashSet<WeightedGraphNode> maximumISet, boolean onlyLiterals) {
		int count = 0;
		BasicPattern patternCopy = new BasicPattern(pattern);
		for (WeightedGraphNode weightedGraphNode : maximumISet) {
			BasicPattern newPattern = HSPBlockPlanner.buildBasicPatternFromWeightedGraphNode(patternCopy, weightedGraphNode.getId());
			newPattern = HSPBlockPlanner.handleCommonVariables(newPattern, weightedGraphNode.getId());
			
			if (onlyLiterals == false) {
				count = countConstants(count, newPattern);
			}
			else{
				count = countLiterals(count, newPattern);
			}
			
			patternCopy.getList().removeAll(newPattern.getList());
		}
		return count;
	}

	private static int countLiterals(int count, BasicPattern newPattern) {
		for (Triple triple : newPattern) {
			if (triple.getSubject().isLiteral()) count++;
			if (triple.getPredicate().isLiteral()) count++;
			if (triple.getObject().isLiteral()) count++;
		}
		return count;
	}

	private static int countConstants(int count, BasicPattern newPattern) {
		for (Triple triple : newPattern) {
			if (triple.getSubject().isConcrete()) count++;
			if (triple.getPredicate().isConcrete()) count++;
			if (triple.getObject().isConcrete()) count++;
		}
		return count;
	}

	static ArrayList<HashSet<WeightedGraphNode>> applyHeuristicH3(BasicPattern pattern, ArrayList<HashSet<WeightedGraphNode>> maximumISets) {
		HashMap<HashSet<WeightedGraphNode>, Integer> distinctPositions = new HashMap<HashSet<WeightedGraphNode>, Integer>();
		ArrayList<Pair<HashSet<WeightedGraphNode>, Integer>> setWeights = new ArrayList<Pair<HashSet<WeightedGraphNode>, Integer>>();
		
		for (HashSet<WeightedGraphNode> hashSet : maximumISets) {
			BasicPattern patternCopy = new BasicPattern(pattern);
			
			int distPositions = 0;
			byte positions = 0;
			int setWeight = 0;
			for (WeightedGraphNode weightedGraphNode : hashSet) {
				
				BasicPattern newPattern = HSPBlockPlanner.buildBasicPatternFromWeightedGraphNode(patternCopy, weightedGraphNode.getId());
				
				for (Triple triple : newPattern) {
					if (triple.getSubject().isVariable()){
						if (triple.getSubject().getName().equals(weightedGraphNode.getId())){
							positions |=  S;
						}
					}
					if (triple.getPredicate().isVariable()){
						if (triple.getPredicate().getName().equals(weightedGraphNode.getId())){
							positions |=  P;
						}
					}
					if (triple.getObject().isVariable()){
						if (triple.getObject().getName().equals(weightedGraphNode.getId())){
							positions |=  O;
						}
					}
				}
				
				setWeight += joinPositionWeights[positions];
				
				for (int i = 0; i < 3; i++) {
					if ((positions & 0x01) == 1)
						distPositions++;
					positions = (byte)((int)positions >> 1);
				}				
				
				patternCopy.getList().removeAll(newPattern.getList());
			}
			
			setWeights.add(new Pair<HashSet<WeightedGraphNode>, Integer>(hashSet, setWeight));
			distinctPositions.put(hashSet, distPositions);
		}
		
		ArrayList<HashSet<WeightedGraphNode>> listOfRemaining = new ArrayList<HashSet<WeightedGraphNode>>();
		buildListOfRemainingFromDistinctPositions(distinctPositions, listOfRemaining);
		
		if (listOfRemaining.size() == maximumISets.size()){
			Collections.sort(setWeights, new Comparator<Pair<HashSet<WeightedGraphNode>, Integer>>(){
				@Override
				public int compare(Pair<HashSet<WeightedGraphNode>, Integer> o1, Pair<HashSet<WeightedGraphNode>, Integer> o2) {
					return (o2.getSecond()-o1.getSecond());
				}
			});
			buildListOfRemainingFromSetWeights(setWeights, listOfRemaining);
			maximumISets.retainAll(listOfRemaining);
		}
		
		return maximumISets;
	}

	private static void buildListOfRemainingFromDistinctPositions(HashMap<HashSet<WeightedGraphNode>, Integer> distinctPositions, ArrayList<HashSet<WeightedGraphNode>> listOfRemaining) {
		int max=0;
		for (Map.Entry<HashSet<WeightedGraphNode>, Integer> entry : distinctPositions.entrySet()) {
			if (entry.getValue()>max){
				max = entry.getValue();
				listOfRemaining.clear();
				listOfRemaining.add(entry.getKey());
			}
			else if (entry.getValue()==max){
				listOfRemaining.add(entry.getKey());
			}
		}
	}

	private static void buildListOfRemainingFromSetWeights(ArrayList<Pair<HashSet<WeightedGraphNode>, Integer>> setWeights, ArrayList<HashSet<WeightedGraphNode>> listOfRemaining) {
		//apply the following order p-o, s-p, s-o, o-o, s-s, p-p
		//assign a weight to each order
		//for each set compute a total weight	
		listOfRemaining.clear();
		listOfRemaining.add(setWeights.get(0).getFirst());
		int refWeight = setWeights.get(0).getSecond();
		int i=1;
		while (i<setWeights.size() && setWeights.get(i).getSecond()==refWeight){
			listOfRemaining.add(setWeights.get(i++).getFirst());
		}
	}

	static ArrayList<HashSet<WeightedGraphNode>> applyHeuristicH4(BasicPattern patternCopy, ArrayList<HashSet<WeightedGraphNode>> maximumISets, ExecutionContext qCxt) {
		Stack<List<Var>> projectionVarStack = (Stack<List<Var>>)qCxt.getContext().get(HBaseSymbols.PROJECTION_VARS);
		List<Var> projectionVars = projectionVarStack.peek();
		HashSet<String> projectionVarSet = new HashSet<String>(projectionVars.size());
		for (Var v : projectionVars) {
			projectionVarSet.add(v.getName());
		}
		
		ArrayList<Pair<HashSet<WeightedGraphNode>, Integer>> setWeightList = new ArrayList<Pair<HashSet<WeightedGraphNode>,Integer>>();
		for (HashSet<WeightedGraphNode> set : maximumISets) {
			int projectionVarCount = 0;
			
			for (WeightedGraphNode weightedGraphNode : set) {
				HashSet<String> localVarNames = new HashSet<String>();
				BasicPattern patternWithVar = HSPBlockPlanner.buildBasicPatternFromWeightedGraphNode(patternCopy, weightedGraphNode.getId());
				
				buildVariableSetFromPattern(localVarNames, patternWithVar);
				
				localVarNames.retainAll(projectionVarSet);
				projectionVarCount += localVarNames.size();
			}
			
			setWeightList.add(new Pair<HashSet<WeightedGraphNode>, Integer>(set, projectionVarCount));
		}
		
		ArrayList<HashSet<WeightedGraphNode>> listOfRemaining = new ArrayList<HashSet<WeightedGraphNode>>();
		Collections.sort(setWeightList, new Comparator<Pair<HashSet<WeightedGraphNode>, Integer>>(){
			@Override
			public int compare(Pair<HashSet<WeightedGraphNode>, Integer> o1, Pair<HashSet<WeightedGraphNode>, Integer> o2) {
				return (o1.getSecond()-o2.getSecond());
			}
		});
		if (setWeightList.get(0).getSecond()!=setWeightList.get(setWeightList.size()-1).getSecond()){
			buildListOfRemainingFromSetWeights(setWeightList, listOfRemaining);
			maximumISets.retainAll(listOfRemaining);
		}
		
		return maximumISets;
	}

	private static void buildVariableSetFromPattern(HashSet<String> localVarNames, BasicPattern patternWithVar) {
		for (Triple triple : patternWithVar) {
			Node subject = triple.getSubject();
			if (subject.isVariable()){
				localVarNames.add(subject.getName());
			}
			Node predicate = triple.getPredicate();
			if (predicate.isVariable()){
				localVarNames.add(predicate.getName());
			}
			
			Node object = triple.getObject();
			if (object.isVariable()){
				localVarNames.add(object.getName());
			}
		}
	}
	
}
