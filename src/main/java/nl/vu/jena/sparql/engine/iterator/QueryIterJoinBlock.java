package nl.vu.jena.sparql.engine.iterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;

import nl.vu.datalayer.hbase.id.Id;
import nl.vu.datalayer.hbase.parameters.ResultRow;
import nl.vu.jena.graph.HBaseGraph;
import nl.vu.jena.sparql.engine.binding.BindingMaterializer;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingBase;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIter1;

public class QueryIterJoinBlock extends QueryIter1 {

	private BasicPattern pattern;
	private Graph graph ;
	private Iterator<ResultRow> resultIter;
	private LinkedHashSet<String> varNames;
	private BindingMaterializer bindingMaterializer;
	
	public QueryIterJoinBlock(QueryIterator input, BasicPattern pattern, ExecutionContext execCxt) {
		super(input, execCxt);
		this.pattern = pattern;
		if (graph instanceof HBaseGraph){
			HBaseGraph hbaseGraph = (HBaseGraph)graph;
			
			//TODO check assumption: varNames and resultIter have results in the same order
			varNames = hbaseGraph.extractVarNamesFromPattern(pattern);
			resultIter = hbaseGraph.getJoinResults(pattern);
			bindingMaterializer = new BindingMaterializer(graph);
		}
	}

	@Override
	protected boolean hasNextBinding() {
		if ( getInput() == null )
            return false ;

        if ( !getInput().hasNext() )
        {
            getInput().close() ;
            return false ; 
        }
        
		return resultIter.hasNext();
	}

	@Override
	protected Binding moveToNextBinding() {
		
		ResultRow row = resultIter.next();
		Binding binding = getInput().next();
		Binding newBinding = BindingFactory.create(binding) ;
		
		Iterator<Id> idIter = row.iterator();
		Iterator<String> varIter = varNames.iterator();
		while (idIter.hasNext() && varIter.hasNext()){
			((BindingMap)newBinding).add(Var.alloc(varIter.next()), 
					Node.createUncachedLiteral(idIter.next(), null));
		}
		
		if (newBinding instanceof BindingBase && graph instanceof HBaseGraph){
        	try {
        		newBinding = bindingMaterializer.materialize(newBinding);
			} catch (IOException e) {
				return null;
			}
        }
		
		return newBinding;
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
