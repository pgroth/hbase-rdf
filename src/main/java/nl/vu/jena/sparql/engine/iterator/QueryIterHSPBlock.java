package nl.vu.jena.sparql.engine.iterator;

import nl.vu.jena.sparql.engine.hsp.HSPBlockPlanner;

import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIter;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIter1;

/**
 * This class solves a Basic Graph Pattern using 
 * a variation of the Heuristics based planner, 
 * backed by a merge-join strategy
 *  
 * @author Sever Fundatureanu
 *
 */
public class QueryIterHSPBlock extends QueryIter1 {

	private QueryIter joinIter;

	public QueryIterHSPBlock(QueryIterator input, BasicPattern pattern, ExecutionContext execCxt) {
		super(input, execCxt);
		
		joinIter = HSPBlockPlanner.buildPlan(pattern, execCxt);
		
		//execute plan recursively
	}

	@Override
	protected void requestSubCancel() {
		// TODO Auto-generated method stub

	}

	@Override
	protected void closeSubIterator() {
		// TODO Auto-generated method stub

	}

	@Override
	protected boolean hasNextBinding() {
		return joinIter.hasNext();
	}

	@Override
	protected Binding moveToNextBinding() {
		return joinIter.nextBinding();
	}

}
