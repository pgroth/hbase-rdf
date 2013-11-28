package nl.vu.jena.sparql.engine.iterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import nl.vu.datalayer.hbase.id.Id;
import nl.vu.datalayer.hbase.parameters.ResultRow;
import nl.vu.jena.graph.HBaseGraph;
import nl.vu.jena.sparql.engine.joinable.JoinEventHandler;
import nl.vu.jena.sparql.engine.joinable.JoinListener;
import nl.vu.jena.sparql.engine.joinable.Joinable;
import nl.vu.jena.sparql.engine.main.HBaseSymbols;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.ARQ;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIter1;

public class QueryIterJoinBlock extends QueryIter1 implements Joinable {

	private Graph graph ;
	private Iterator<Binding> joinedResultsIter=null;
	private Iterator<ResultRow> resultIter;
	private LinkedHashSet<String> varNames;
	private BasicPattern pattern;
	private short joinId;
	
	private JoinEventHandler joinEventHandler;
	
	public QueryIterJoinBlock(QueryIterator input, BasicPattern pattern, ExecutionContext execCxt, short joinId) {
		super(input, execCxt);

		joinEventHandler = new JoinEventHandler((ExecutorService)ARQ.getContext().get(HBaseSymbols.EXECUTOR), this);
		
		graph = execCxt.getActiveGraph();
		this.pattern = pattern;
		this.joinId = joinId;
		
	}

	@Override
	public void setParent(JoinListener parent) {
		if (parent != null){
			joinEventHandler.registerListener(parent);
		}
	}

	@Override
	public void run() {
		HBaseGraph hbaseGraph = (HBaseGraph) graph;

		varNames = hbaseGraph.extractVarNamesFromPattern(pattern, joinId);
		resultIter = hbaseGraph.getJoinResults(pattern);
		
		ArrayList<Binding> results = new ArrayList<Binding>();
		while (resultIter.hasNext()){
			ResultRow row = resultIter.next();
			Binding newBinding = BindingFactory.create(BindingFactory.root());
			
			Iterator<Id> idIter = row.iterator();
			Iterator<String> varIter = varNames.iterator();
			while (idIter.hasNext() && varIter.hasNext()){
				((BindingMap)newBinding).add(Var.alloc(varIter.next()), 
						Node.createUncachedLiteral(idIter.next(), null));
			}
			
			results.add(newBinding);
		}
		
		joinedResultsIter = results.iterator();
		
		joinEventHandler.notifyListeners();
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
	protected void requestSubCancel() {
		// TODO Auto-generated method stub

	}

	@Override
	protected void closeSubIterator() {
		// TODO Auto-generated method stub

	}

	

}
