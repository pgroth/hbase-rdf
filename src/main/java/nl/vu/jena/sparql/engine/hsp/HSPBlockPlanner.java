package nl.vu.jena.sparql.engine.hsp;

import java.util.ArrayList;
import java.util.HashSet;

import nl.vu.jena.sparql.engine.iterator.QueryIterCartesianProduct;
import nl.vu.jena.sparql.engine.iterator.QueryIterHashJoin;
import nl.vu.jena.sparql.engine.iterator.QueryIterJoinBlock;
import nl.vu.jena.sparql.engine.iterator.TripleMapper;
import nl.vu.jena.sparql.engine.joinable.JoinListener;
import nl.vu.jena.sparql.engine.joinable.Joinable;
import nl.vu.jena.sparql.engine.joinable.TwoWayJoinable;

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
			ArrayList<String> commonVars = getCommonVariables((Joinable)mergeJoinBlocks.get(i), (Joinable)mergeJoinBlocks.get(i+1));
			QueryIter newNode = buildNewNode(mergeJoinBlocks.get(i), mergeJoinBlocks.get(i+1), qCxt, commonVars);
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

		ArrayList<QueryIter> mergeJoinBlocks;
		
		if (pattern.size() > 1) {
			VariableGraph varGraph = new VariableGraph(pattern);

			ArrayList<HashSet<WeightedGraphNode>> maximumISets = MaximumIndependentSet.computeSets(varGraph);

			HashSet<WeightedGraphNode> maximumISet = null;
			if (maximumISets.size() > 1) {
				
				maximumISets = applyHeuristicH1H2(pattern, maximumISets, maximumISet, false);
				
				if (maximumISets.size()>1){
					
					maximumISets = applyHeuristicH1H2(pattern, maximumISets, maximumISet, true);
					
					//TODO apply remaining heuristics
					
				}					
			}
			
			maximumISet = maximumISets.get(0);
			

			mergeJoinBlocks = buildMergeJoinBlocksFromMaxIndependentSet(pattern, qCxt, maximumISet);
		}
		else{
			mergeJoinBlocks = new ArrayList<QueryIter>();
		}
		
		for (Triple triple : pattern) {
			QueryIter tp =  new TripleMapper(BindingRoot.create(), triple, qCxt);
			mergeJoinBlocks.add(tp);
		}
		
		return mergeJoinBlocks;
	}

	/**
	 * Choose the patterns which have the most number of constants (BNodes, URIs or Literals)
	 * 
	 * @param pattern
	 * @param maximumISets
	 * @param maximumISet
	 * @return
	 */
	private static ArrayList<HashSet<WeightedGraphNode>> applyHeuristicH1H2(BasicPattern pattern, ArrayList<HashSet<WeightedGraphNode>> maximumISets, HashSet<WeightedGraphNode> maximumISet, boolean onlyLiterals) {
		int maxConcrete = 0;
		ArrayList<HashSet<WeightedGraphNode>> newMaxISets = new ArrayList<HashSet<WeightedGraphNode>>();
		for (HashSet<WeightedGraphNode> maxISet : maximumISets) {
			
			int countConcrete = countConcrete(pattern, maximumISet, onlyLiterals);
			
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
			BasicPattern newPattern = buildBasicPatternFromWeightedGraphNode(patternCopy, weightedGraphNode);
			
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

	private static ArrayList<QueryIter> buildMergeJoinBlocksFromMaxIndependentSet(BasicPattern pattern, ExecutionContext qCxt, HashSet<WeightedGraphNode> maximumISet) {
		ArrayList<QueryIter> mergeJoinBlocks = new ArrayList<QueryIter>(maximumISet.size());

		for (WeightedGraphNode weightedGraphNode : maximumISet) {
			BasicPattern newPattern = buildBasicPatternFromWeightedGraphNode(pattern, weightedGraphNode);
			
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
	}

	private static BasicPattern buildBasicPatternFromWeightedGraphNode(BasicPattern pattern, WeightedGraphNode weightedGraphNode) {
		BasicPattern newPattern = new BasicPattern();
		
		for (Triple triple : pattern) {
			if (triple.getSubject().isVariable()){
				if (triple.getSubject().getName().equals(weightedGraphNode.getId())){
					newPattern.add(triple);
					continue;
				}		
			}
			
			if (triple.getPredicate().isVariable()){
				if (triple.getPredicate().getName().equals(weightedGraphNode.getId())){
					newPattern.add(triple);
					continue;
				}		
			}
			
			if (triple.getObject().isVariable()){
				if (triple.getObject().getName().equals(weightedGraphNode.getId())){
					newPattern.add(triple);
					continue;
				}		
			}
		}
		return newPattern;
	}

}
