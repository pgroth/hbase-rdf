package nl.vu.jena.sparql.engine.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;

import nl.vu.jena.sparql.engine.binding.BindingMaterializer;
import nl.vu.jena.sparql.engine.hsp.HSPBlockPlanner;
import nl.vu.jena.sparql.engine.joinable.JoinEvent;
import nl.vu.jena.sparql.engine.joinable.JoinListener;
import nl.vu.jena.sparql.engine.joinable.Joinable;
import nl.vu.jena.sparql.engine.main.HBaseSymbols;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.query.ARQ;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;
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
public class QueryIterHSPBlock extends QueryIter1 implements JoinListener{
	
	public static ExecutorService executor = null;
	
	private Binding parentBinding = null;
	private BasicPattern pattern;
	private ExecutionContext execCxt;
	
	private Iterator<Binding> materializedJoinIter;
	private volatile boolean finished = false;

	public QueryIterHSPBlock(QueryIterator input, BasicPattern pattern, ExecutionContext execCxt) {
		super(input, execCxt);
		
		if (input.hasNext()) {
			parentBinding = input.next();
		}
		this.pattern = pattern;
		this.execCxt = execCxt;
	}
	
	public void runJoins() throws IOException{
		ArrayList<QueryIter> mergeJoinBlocks = HSPBlockPlanner.getMergeJoinBlocks(pattern, execCxt); 
		QueryIter joinIter = HSPBlockPlanner.buildTreeBottomUp(mergeJoinBlocks, execCxt);	
		((Joinable)joinIter).setParent((JoinListener)this);
		
		waitForJoins(mergeJoinBlocks);
				
		BindingMaterializer bindingMaterializer = new BindingMaterializer(execCxt.getActiveGraph());
		materializedJoinIter = bindingMaterializer.materialize(joinIter).iterator();
	}

	private void waitForJoins(ArrayList<QueryIter> mergeJoinBlocks) {
		ExecutorService executorService = (ExecutorService)ARQ.getContext().get(HBaseSymbols.EXECUTOR);
		for (QueryIter queryIter : mergeJoinBlocks) {//start running merge join blocks on separate threads
			executorService.execute((Runnable)queryIter);
		}
		
		synchronized(this){//wait for joins to finish
			if (!finished){
				try {
					this.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	protected boolean hasNextBinding() {
		if (getInput() == null){
			return false;
		}

		if (parentBinding == null) {
			getInput().close();
			return false;
		}
		
		return materializedJoinIter.hasNext();
	}

	@Override
	protected Binding moveToNextBinding() {
		BindingMap newBinding = BindingFactory.create(parentBinding);
		newBinding.addAll(materializedJoinIter.next());
		
		return newBinding;
	}

	@Override
	public void joinFinished(JoinEvent e) {
		synchronized(this){
			finished=true;
			this.notify();
		}		
	}

	@Override
	protected void requestSubCancel() {
	}

	@Override
	protected void closeSubIterator() {
	}
}
