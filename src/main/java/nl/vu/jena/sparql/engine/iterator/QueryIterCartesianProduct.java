package nl.vu.jena.sparql.engine.iterator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import nl.vu.jena.sparql.engine.joinable.JoinEvent;
import nl.vu.jena.sparql.engine.joinable.JoinEventHandler;
import nl.vu.jena.sparql.engine.joinable.JoinListener;
import nl.vu.jena.sparql.engine.joinable.Joinable;
import nl.vu.jena.sparql.engine.joinable.TwoWayJoinable;
import nl.vu.jena.sparql.engine.main.HBaseSymbols;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.ARQ;
import com.hp.hpl.jena.sparql.algebra.Table;
import com.hp.hpl.jena.sparql.algebra.TableFactory;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIter2;

/**
 * Used when we know there are no intersecting columns between left and right
 *
 */
public class QueryIterCartesianProduct extends QueryIter2 implements TwoWayJoinable, JoinListener {
	
	private Iterator<Binding> joinedResultsIter=null;
	private QueryIterator current = null ;
	private HashSet<String> varNames;
	private Table tableRight ;          // Materialized iterator
	private JoinEventHandler joinEventHandler;
	
	public QueryIterCartesianProduct(QueryIterator left, QueryIterator right, ExecutionContext execCxt) {
		super(left, right, execCxt);
		
		joinEventHandler = new JoinEventHandler((ExecutorService)ARQ.getContext().get(HBaseSymbols.EXECUTOR), this);
		buildVarNames((Joinable)getLeft(), (Joinable)getRight());
	}
	
	@Override
	public void run() {
		tableRight = TableFactory.create(getRight()) ;
		getRight().close();
		
		ArrayList<Binding> joinedResults = buildJoinedResults();
		
		super.closeIterator();
		
		joinedResultsIter = joinedResults.iterator();
		
		joinEventHandler.notifyListeners();
	}

	private ArrayList<Binding> buildJoinedResults() {
		ArrayList<Binding> joinedResults = new ArrayList<Binding>();
		QueryIterator left = getLeft();

		while (left.hasNext()){
			if (isFinished())
	            break;
	        
			Binding leftBinding = left.nextBinding();
			
			QueryIterator rightIterator = tableRight.iterator(null);
			while (rightIterator.hasNext()){
				Binding nextBinding = mergeBindings(leftBinding, rightIterator.nextBinding()) ;
				joinedResults.add(nextBinding);				
			}     
		}
		return joinedResults;
	}
	
	private Binding mergeBindings(Binding leftBinding, Binding rightBinding) {
		BindingMap newBinding = BindingFactory.create(leftBinding) ;
        for (Iterator<Var> vIter = rightBinding.vars() ; vIter.hasNext() ;)
        {
            Var v = vIter.next();
            Node n = rightBinding.get(v) ;
            newBinding.add(v, n) ;
        }
		return newBinding;
	}

	private void buildVarNames(Joinable left, Joinable right) {
		varNames = new HashSet<String>();
		for (String string : left.getVarNames()) {
			varNames.add(string);
		}
		for (String string : right.getVarNames()) {
			varNames.add(string);
		}
	}

	@Override
	protected boolean hasNextBinding() {
		return joinedResultsIter.hasNext();
	}

	@Override
	protected Binding moveToNextBinding() {
		return joinedResultsIter.next();
	}

	@Override
	public Set<String> getVarNames() {
		return varNames;
	}

	@Override
	public void setParent(JoinListener parent) {
		if (parent!=null){
			joinEventHandler.registerListener(parent);
		}
	}

	@Override
	public Joinable getLeftJ() {
		return (Joinable)getLeft();
	}

	@Override
	public Joinable getRightJ() {
		return (Joinable)getRight();
	}

	@Override
	public void joinFinished(JoinEvent e) {
		joinEventHandler.joinFinished(e);
	}

	@Override
	protected void requestSubCancel() {
	}

	@Override
	protected void closeSubIterator() {
	}	

}
