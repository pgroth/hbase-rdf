package nl.vu.jena.sparql.engine.hsp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import nl.vu.jena.sparql.engine.iterator.QueryIterCartesianProduct;
import nl.vu.jena.sparql.engine.iterator.QueryIterHashJoin;
import nl.vu.jena.sparql.engine.iterator.QueryIterJoinBlock;
import nl.vu.jena.sparql.engine.iterator.TripleMapper;
import nl.vu.jena.sparql.engine.joinable.JoinListener;
import nl.vu.jena.sparql.engine.joinable.Joinable;
import nl.vu.jena.sparql.engine.joinable.TwoWayJoinable;


import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.binding.BindingRoot;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIter;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterNullIterator;

/**
 * Handles HSP planning at the BGP level
 * 
 * @author Sever Fundatureanu
 *
 */
public class HSPBlockPlanner {
	
	private static short currentJoinId = 0;//TODO background thread which resets this when JOIN table is reset
	
	public static QueryIter buildPlan(BasicPattern pattern, ExecutionContext qCxt){
		
		ArrayList<QueryIter> mergeJoinBlocks = getMergeJoinBlocks(pattern, qCxt); 
		
		QueryIter queryIter = buildTreeBottomUp(mergeJoinBlocks, qCxt);	
		return queryIter;
	}

	public static QueryIter buildTreeBottomUp(ArrayList<QueryIter> mergeJoinBlocks, ExecutionContext qCxt) {		
		QueryIter root = null;
		
		if (mergeJoinBlocks.size()==1){
			root = mergeJoinBlocks.get(0);
		}
		else{		
			root = buildTreeBottomUpRecursively(mergeJoinBlocks, qCxt);
		}
		return root;
	}

	private static QueryIter buildTreeBottomUpRecursively(ArrayList<QueryIter> mergeJoinBlocks, ExecutionContext qCxt) {
		int listSize = mergeJoinBlocks.size();
		if (listSize==1){
			return mergeJoinBlocks.get(0);
		}
		
		ArrayList<QueryIter> levelUp = new ArrayList<QueryIter>();
		
		int evenSize = listSize%2==1 ? (listSize-1) : listSize;
		for (int i = 0; i<evenSize; i+=2) {
			QueryIter left = mergeJoinBlocks.get(i);
			QueryIter right = mergeJoinBlocks.get(i+1);
			ArrayList<String> commonVars = getCommonVariables((Joinable)left, (Joinable)right);
			QueryIter newNode = buildNewNode(left, right, qCxt, commonVars);
			levelUp.add(newNode);
		}
		
		if (listSize%2==1){
			levelUp.add(mergeJoinBlocks.get(listSize-1));
		}
		
		makeConnectionsFromChildrenToParents(levelUp);
		
		QueryIter root = buildTreeBottomUpRecursively(levelUp, qCxt);
		
		return root;
	}

	private static void makeConnectionsFromChildrenToParents(
			ArrayList<QueryIter> levelUp) {
		for (QueryIter queryIter : levelUp) {
			if (queryIter instanceof TwoWayJoinable){
				Joinable left = ((TwoWayJoinable)queryIter).getLeftJ();
				left.setParent((JoinListener)queryIter);
				
				Joinable right = ((TwoWayJoinable)queryIter).getRightJ();
				right.setParent((JoinListener)queryIter);
			}
		}
	}

	private static QueryIter buildNewNode(QueryIter left, QueryIter right, ExecutionContext qCxt, ArrayList<String> commonVars) {
		QueryIter root;
		if (commonVars.size()>0){
			root = new QueryIterHashJoin(left, right, commonVars, qCxt);
		}
		else{
			root = new QueryIterCartesianProduct(left, right, qCxt);
		}
		return root;
	}

	private static ArrayList<String> getCommonVariables(Joinable left, Joinable right) {
		ArrayList<String> common = new ArrayList<String>();
		common.addAll(left.getVarNames());
		common.retainAll(right.getVarNames());
		return common;
	}

	public static ArrayList<QueryIter> getMergeJoinBlocks(BasicPattern pattern, ExecutionContext qCxt) {

		ArrayList<QueryIter> mergeJoinBlocks = new ArrayList<QueryIter>();
		HashSet<WeightedGraphNode> maximumISet = null;
		
		if (pattern.size() > 1) {
			BasicPattern patternCopy = new BasicPattern(pattern);
			
			while (patternCopy.size() > 0) {
				VariableGraph varGraph = new VariableGraph(patternCopy);

				ArrayList<HashSet<WeightedGraphNode>> maximumISets = MaximumIndependentSet.computeSets(varGraph);

				maximumISets = applyHeuristics(patternCopy, maximumISets, qCxt);

				maximumISet = maximumISets.get(0);
				Iterator<WeightedGraphNode> iterator = maximumISet.iterator();
				if (!iterator.hasNext()) {
					break;
				}
				WeightedGraphNode weightedGraphNode = iterator.next();

				BasicPattern newPattern = buildBasicPatternFromWeightedGraphNode(patternCopy, weightedGraphNode.getId());
				newPattern = handleCommonVariables(newPattern, weightedGraphNode.getId());

				if (newPattern.size() > 0) {
					QueryIter basicBlock;
					if (newPattern.size() > 1) {
						basicBlock = new QueryIterJoinBlock(new QueryIterNullIterator(qCxt), newPattern, qCxt, currentJoinId++);
					} 
					else {
						basicBlock = new TripleMapper(BindingRoot.create(), newPattern.get(0), qCxt);
					}
					mergeJoinBlocks.add(basicBlock);
				}
				
				patternCopy.getList().removeAll(newPattern.getList());
			}

			//mergeJoinBlocks = buildMergeJoinBlocksFromMaxIndependentSet(pattern, qCxt, finalSet);
		}
		else{
			QueryIter tp =  new TripleMapper(BindingRoot.create(), pattern.get(0), qCxt);
			mergeJoinBlocks.add(tp);
		}
		
		return mergeJoinBlocks;
	}

	private static ArrayList<HashSet<WeightedGraphNode>> applyHeuristics(BasicPattern patternCopy, ArrayList<HashSet<WeightedGraphNode>> maximumISets, ExecutionContext qCxt) {
		if (maximumISets.size() > 1) {
			maximumISets = HSPHeuristics.applyHeuristicH1H2(patternCopy, maximumISets, false);

			if (maximumISets.size() > 1) {

				maximumISets = HSPHeuristics.applyHeuristicH1H2(patternCopy, maximumISets, true);

				if (maximumISets.size() > 1) {
					
					maximumISets = HSPHeuristics.applyHeuristicH3(patternCopy, maximumISets);
					
					if (maximumISets.size() > 1){
						maximumISets = HSPHeuristics.applyHeuristicH4(patternCopy, maximumISets, qCxt);
					}
				}

			}
		}
		return maximumISets;
	}

	static BasicPattern handleCommonVariables(BasicPattern newPattern, String varNodeName) {
		//count how many times each variable appears 
		//if there are triple patterns which don't have any other variables, or which have variables which appear in only 1 tp,
		//		build a block from these triple patterns
		//if there are multiple common variables, other than the primary one, 
		//	choose the triple patterns with the least common variable (appears in the least nr of tp)
		
		String specialVar = "__DUMMY";
		LinkedHashMap<String, Integer> varCounts = new LinkedHashMap<String, Integer>();
		for (Triple triple : newPattern) {
			byte updated=0;
			updated += updateVarCountMap(varCounts, varNodeName, triple.getSubject());
			updated += updateVarCountMap(varCounts, varNodeName, triple.getPredicate());
			updated += updateVarCountMap(varCounts, varNodeName, triple.getObject());
			
			if (updated == 0){
				varCounts.put(specialVar, 1);
			}
		}
		
		List<Map.Entry<String, Integer>> varCountList = new ArrayList<Map.Entry<String, Integer>>(varCounts.entrySet());
		Collections.sort(varCountList, new Comparator<Map.Entry<String, Integer>>(){
			@Override
			public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
				return (o2.getValue()-o1.getValue());
			}
		});
		
		if (varCountList.size()==0 || varCountList.get(0).getValue()<=1){
			//we don't have any other common variables between triple patterns
			return newPattern;
		}
		else {//preserve only the triple patterns with the least common variable			
			int smallest = varCountList.get(varCountList.size()-1).getValue();
			updatePattern(newPattern, varNodeName, varCounts, smallest);
			return newPattern;
		}
		
	}

	private static void updatePattern(BasicPattern newPattern, String varNodeName, LinkedHashMap<String, Integer> varCounts, int smallest) {
		List<Triple> toDelete = new ArrayList<Triple>();
		for (Triple t : newPattern) {
			Node subject = t.getSubject();
			if (subject.isVariable() && !varNodeName.equals(subject.getName())){
				if (varCounts.get(subject.getName())>smallest){
					toDelete.add(t);
					continue;
				}
			}
			
			Node predicate = t.getPredicate();
			if (predicate.isVariable() && !varNodeName.equals(predicate.getName())){
				if (varCounts.get(predicate.getName())>smallest){
					toDelete.add(t);
					continue;
				}
			}
			
			Node object = t.getObject();
			if (object.isVariable() && !varNodeName.equals(object.getName())){
				if (varCounts.get(object.getName())>smallest){
					toDelete.add(t);
					continue;
				}
			}
		}
		
		newPattern.getList().removeAll(toDelete);
	}
	
	private static byte updateVarCountMap(LinkedHashMap<String, Integer> varCounts, String varNodeName, Node node) {
		if (node.isVariable()){
			String nodeName = node.getName();
			if (!nodeName.equals(varNodeName)){
				Integer varCount = varCounts.get(nodeName);
				if (varCount==null){
					varCounts.put(nodeName, 1);
				}
				else{
					varCounts.put(nodeName, varCount+1);
				}
				return 1;
			}		
		}
		return 0;
	}
	
	static BasicPattern buildBasicPatternFromWeightedGraphNode(BasicPattern pattern, String varNodeName) {
		BasicPattern newPattern = new BasicPattern();
		
		for (Triple triple : pattern) {
			if (triple.getSubject().isVariable()){
				if (triple.getSubject().getName().equals(varNodeName)){
					newPattern.add(triple);
					continue;
				}		
			}
			
			if (triple.getPredicate().isVariable()){
				if (triple.getPredicate().getName().equals(varNodeName)){
					newPattern.add(triple);
					continue;
				}		
			}
			
			if (triple.getObject().isVariable()){
				if (triple.getObject().getName().equals(varNodeName)){
					newPattern.add(triple);
					continue;
				}		
			}
		}
		
		return newPattern;
	}

	/*private static ArrayList<QueryIter> buildMergeJoinBlocksFromMaxIndependentSet(BasicPattern pattern, ExecutionContext qCxt, HashSet<WeightedGraphNode> maximumISet) {
		ArrayList<QueryIter> mergeJoinBlocks = new ArrayList<QueryIter>(maximumISet.size());

		for (WeightedGraphNode weightedGraphNode : maximumISet) {
			BasicPattern newPattern = buildBasicPatternFromWeightedGraphNode(pattern, weightedGraphNode.getId());
			newPattern = handleCommonVariables(newPattern, weightedGraphNode.getId());
			
			if (newPattern.size() > 0) {
				QueryIter basicBlock;
				if (newPattern.size() > 1) {
					basicBlock = new QueryIterJoinBlock(new QueryIterNullIterator(qCxt), newPattern, qCxt, currentJoinId++);
				} 
				else {
					basicBlock = new TripleMapper(BindingRoot.create(), newPattern.get(0), qCxt);
				}

				mergeJoinBlocks.add(basicBlock);
				pattern.getList().removeAll(newPattern.getList());
			}
		}
		
		return mergeJoinBlocks;
	}*/
	

}
