package nl.vu.jena.sparql.engine.iterator;

import java.io.IOException;

import nl.vu.jena.graph.HBaseGraph;
import nl.vu.jena.sparql.engine.binding.BindingMaterializer;
import nl.vu.jena.sparql.engine.hsp.HSPBlockPlanner;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingBase;
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
	private BindingMaterializer bindingMaterializer;
	private Binding parentBinding = null;
	private Graph graph ;

	public QueryIterHSPBlock(QueryIterator input, BasicPattern pattern, ExecutionContext execCxt) {
		super(input, execCxt);
		
		if (input.hasNext()) {
			parentBinding = input.next();
		}
		
		joinIter = HSPBlockPlanner.buildPlan(pattern, execCxt);
		
		//TODO execute plan recursively
		
		graph = execCxt.getActiveGraph();
		if (graph instanceof HBaseGraph) {
			bindingMaterializer = new BindingMaterializer(graph);
		}
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
		if (getInput() == null)
			return false;

		if (parentBinding == null) {
			getInput().close();
			return false;
		}
		
		return joinIter.hasNext();
	}

	@Override
	protected Binding moveToNextBinding() {
		Binding newBinding = joinIter.nextBinding();
		
		if (newBinding instanceof BindingBase && graph instanceof HBaseGraph){
        	try {
        		newBinding = bindingMaterializer.materialize(newBinding);
			} catch (IOException e) {
				return null;
			}
        }
		
		return newBinding;
	}

}
