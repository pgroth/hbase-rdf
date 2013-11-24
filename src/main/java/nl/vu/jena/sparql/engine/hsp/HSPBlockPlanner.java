package nl.vu.jena.sparql.engine.hsp;

import java.util.ArrayList;
import java.util.HashSet;

import nl.vu.jena.sparql.engine.iterator.QueryIterJoinBlock;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIter;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterNullIterator;
import com.hp.hpl.jena.sparql.engine.main.iterator.QueryIterJoin;

/**
 * Handles HSP planning at the BGP level
 * 
 * @author Sever Fundatureanu
 *
 */
public class HSPBlockPlanner {
	
	private static short currentJoinId = 0;//TODO background thread which resets this when JOIN table is reset
	
	public static QueryIter buildPlan(BasicPattern pattern, ExecutionContext qCxt){
		
		ArrayList<QueryIterJoinBlock> mergeJoinBlocks = getMergeJoinBlocks(pattern, qCxt); 
		
		QueryIter queryIter = buildTreeBottomUp(mergeJoinBlocks, qCxt);	
		return queryIter;
	}

	private static QueryIter buildTreeBottomUp(ArrayList<QueryIterJoinBlock> mergeJoinBlocks, ExecutionContext qCxt) {		
		QueryIter root = null;
		
		if (mergeJoinBlocks.size()==1){
			root = mergeJoinBlocks.get(0);
		}
		else{
			root = new QueryIterJoin(mergeJoinBlocks.get(0), mergeJoinBlocks.get(1), qCxt);
			for (int i = 1; i < mergeJoinBlocks.size(); i++) {
				root = new QueryIterJoin(root, mergeJoinBlocks.get(i), qCxt);//TODO these have to become hash joins or merge joins type 2
			}
		}
		return root;
	}

	private static ArrayList<QueryIterJoinBlock> getMergeJoinBlocks(BasicPattern pattern, ExecutionContext qCxt) {

		VariableGraph varGraph = new VariableGraph(pattern);
		
		ArrayList<HashSet<WeightedGraphNode>> maximumISets = MaximumIndependentSet.computeSets(varGraph);
		
		HashSet<WeightedGraphNode> maximumISet = maximumISets.get(0);
		if (maximumISets.size()>1){
			//TODO apply heuristics to select the one which provides the smallest number of intermediate results
		}
		
		ArrayList<QueryIterJoinBlock> mergeJoinBlocks = buildMergeJoinBlocksFromMaxIndependentSet(pattern, qCxt, maximumISet);
		
		return mergeJoinBlocks;
	}

	private static ArrayList<QueryIterJoinBlock> buildMergeJoinBlocksFromMaxIndependentSet(BasicPattern pattern, ExecutionContext qCxt, HashSet<WeightedGraphNode> maximumISet) {
		ArrayList<QueryIterJoinBlock> mergeJoinBlocks = new ArrayList<QueryIterJoinBlock>(maximumISet.size());

		for (WeightedGraphNode weightedGraphNode : maximumISet) {
			BasicPattern newPattern = buildBasicPatternWithVariableNode(pattern, weightedGraphNode);
			
			QueryIterJoinBlock joinBlock = new QueryIterJoinBlock(new QueryIterNullIterator(qCxt), 
																	newPattern, qCxt, currentJoinId++);
			mergeJoinBlocks.add(joinBlock);
		}
		return mergeJoinBlocks;
	}

	private static BasicPattern buildBasicPatternWithVariableNode(BasicPattern pattern, WeightedGraphNode weightedGraphNode) {
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
