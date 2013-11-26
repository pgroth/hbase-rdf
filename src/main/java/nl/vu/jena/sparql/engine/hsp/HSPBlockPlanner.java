package nl.vu.jena.sparql.engine.hsp;

import java.util.ArrayList;
import java.util.HashSet;

import nl.vu.jena.sparql.engine.iterator.Joinable;
import nl.vu.jena.sparql.engine.iterator.QueryIterCartesianProduct;
import nl.vu.jena.sparql.engine.iterator.QueryIterHashJoin;
import nl.vu.jena.sparql.engine.iterator.QueryIterJoinBlock;
import nl.vu.jena.sparql.engine.iterator.TripleMapper;

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

	private static QueryIter buildTreeBottomUp(ArrayList<QueryIter> mergeJoinBlocks, ExecutionContext qCxt) {		
		QueryIter root = null;
		
		if (mergeJoinBlocks.size()==1){
			root = mergeJoinBlocks.get(0);
		}
		else{
			ArrayList<String> commonVars = getCommonVariables((Joinable)mergeJoinBlocks.get(0), (Joinable)mergeJoinBlocks.get(1));
			if (commonVars.size()>0){
				root = new QueryIterHashJoin(mergeJoinBlocks.get(0), mergeJoinBlocks.get(1), commonVars, qCxt);
			}
			else{
				root = new QueryIterCartesianProduct(mergeJoinBlocks.get(0), mergeJoinBlocks.get(1), qCxt);
			}
			
			for (int i = 2; i < mergeJoinBlocks.size(); i++) {
				commonVars = getCommonVariables((Joinable)root, (Joinable)mergeJoinBlocks.get(i));
				if (commonVars.size()>0){
					root = new QueryIterHashJoin(root, mergeJoinBlocks.get(i), commonVars, qCxt);
				}
				else{
					root = new QueryIterCartesianProduct(mergeJoinBlocks.get(0), mergeJoinBlocks.get(1), qCxt);
				}
			}
		}
		return root;
	}

	private static ArrayList<String> getCommonVariables(Joinable left, Joinable right) {
		ArrayList<String> common = new ArrayList<String>();
		common.addAll(left.getVarNames());
		common.retainAll(right.getVarNames());
		return common;
	}

	private static ArrayList<QueryIter> getMergeJoinBlocks(BasicPattern pattern, ExecutionContext qCxt) {

		VariableGraph varGraph = new VariableGraph(pattern);
		
		ArrayList<HashSet<WeightedGraphNode>> maximumISets = MaximumIndependentSet.computeSets(varGraph);
		
		HashSet<WeightedGraphNode> maximumISet = maximumISets.get(0);
		if (maximumISets.size()>1){
			//TODO apply heuristics to select the one which provides the smallest number of intermediate results
		}
		
		ArrayList<QueryIter> mergeJoinBlocks = buildMergeJoinBlocksFromMaxIndependentSet(pattern, qCxt, maximumISet);
		for (Triple triple : pattern) {
			QueryIter tp =  new TripleMapper(BindingRoot.create(), triple, qCxt);
			mergeJoinBlocks.add(tp);
		}
		
		return mergeJoinBlocks;
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
