package nl.vu.jena.sparql.engine.iterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashSet;

import nl.vu.datalayer.hbase.id.Id;
import nl.vu.datalayer.hbase.parameters.ResultRow;
import nl.vu.jena.graph.HBaseGraph;
import nl.vu.jena.sparql.engine.binding.BindingMaterializer;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
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
	private Binding parentBinding = null;
	private Iterator<ResultRow> resultIter;
	private LinkedHashSet<String> varNames;
	private BindingMaterializer bindingMaterializer;
	
	public QueryIterJoinBlock(QueryIterator input, BasicPattern pattern, ExecutionContext execCxt, short joinId) {
		super(input, execCxt);
		
		if (input.hasNext()) {
			parentBinding = input.next();

			this.pattern = pattern;

			graph = execCxt.getActiveGraph();
			if (graph instanceof HBaseGraph) {
				HBaseGraph hbaseGraph = (HBaseGraph) graph;

				varNames = hbaseGraph.extractVarNamesFromPattern(pattern,
						joinId);
				resultIter = hbaseGraph.getJoinResults(pattern);
				bindingMaterializer = new BindingMaterializer(graph);
			}
		}
	}

	@Override
	protected boolean hasNextBinding() {
		if ( getInput() == null )
            return false ;
		
        if ( parentBinding == null )
        {
            getInput().close();
            return false ; 
        }
        
		return resultIter.hasNext();
	}

	@Override
	protected Binding moveToNextBinding() {
		
		ResultRow row = resultIter.next();
		Binding newBinding = BindingFactory.create(parentBinding) ;
		
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
