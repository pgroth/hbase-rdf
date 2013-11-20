package nl.vu.jena.sparql.engine.iterator;

import java.util.ArrayList;

import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIter;
import com.hp.hpl.jena.sparql.engine.main.iterator.QueryIterJoin;
import com.hp.hpl.jena.sparql.engine.main.iterator.QueryIterJoinBase;



/**
 * Handles HSP planning at the BGP level
 * 
 * @author Sever Fundatureanu
 *
 */
public class HSPBlockPlanner {
	
	public static QueryIter buildPlan(BasicPattern pattern, ExecutionContext qCxt){
		
		ArrayList<QueryIterJoinBlock> mergeJoinBlocks = getMergeJoinBlocks(pattern); 
		
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

	private static ArrayList<QueryIterJoinBlock> getMergeJoinBlocks(BasicPattern pattern) {

		//build variable graph
		
		//compute maximum independent set
		
		//apply heuristics to select the one which provides the smallest number of intermediate results
		
		//build merge join blocks from variable graph
		return null;
	}

}
