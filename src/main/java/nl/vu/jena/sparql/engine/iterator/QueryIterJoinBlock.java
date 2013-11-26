package nl.vu.jena.sparql.engine.iterator;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import nl.vu.datalayer.hbase.id.Id;
import nl.vu.datalayer.hbase.parameters.ResultRow;
import nl.vu.jena.graph.HBaseGraph;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
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
	private Iterator<ResultRow> resultIter;
	private LinkedHashSet<String> varNames;
	
	public QueryIterJoinBlock(QueryIterator input, BasicPattern pattern, ExecutionContext execCxt, short joinId) {
		super(input, execCxt);

		graph = execCxt.getActiveGraph();
		if (graph instanceof HBaseGraph) {
			HBaseGraph hbaseGraph = (HBaseGraph) graph;

			varNames = hbaseGraph.extractVarNamesFromPattern(pattern, joinId);
			resultIter = hbaseGraph.getJoinResults(pattern);
		}
	}

	@Override
	protected boolean hasNextBinding() {
		return resultIter.hasNext();
	}

	@Override
	protected Binding moveToNextBinding() {
		
		ResultRow row = resultIter.next();
		Binding newBinding = BindingFactory.create(BindingFactory.root());
		
		Iterator<Id> idIter = row.iterator();
		Iterator<String> varIter = varNames.iterator();
		while (idIter.hasNext() && varIter.hasNext()){
			((BindingMap)newBinding).add(Var.alloc(varIter.next()), 
					Node.createUncachedLiteral(idIter.next(), null));
		}
		
		return newBinding;
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
