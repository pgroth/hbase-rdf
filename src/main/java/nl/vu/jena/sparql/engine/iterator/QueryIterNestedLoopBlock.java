package nl.vu.jena.sparql.engine.iterator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import nl.vu.jena.graph.FilteredTriple;
import nl.vu.jena.graph.HBaseGraph;
import nl.vu.jena.sparql.engine.binding.BindingMaterializer;

import org.openjena.atlas.io.IndentedWriter;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_Literal;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.JenaException;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingBase;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIter1;
import com.hp.hpl.jena.sparql.serializer.SerializationContext;
import com.hp.hpl.jena.sparql.util.Utils;

public class QueryIterNestedLoopBlock extends QueryIter1
{
    private Graph graph ;
    private QueryIterator output ;
    private BindingMaterializer bindingMaterializer;
   
    
    public static QueryIterator create(QueryIterator input,
                                       BasicPattern pattern , 
                                       ExecutionContext execContext)
    {
        return new QueryIterNestedLoopBlock(input, pattern, execContext) ;
    }
    
    private QueryIterNestedLoopBlock(QueryIterator input,
                                    BasicPattern pattern , 
                                    ExecutionContext execContext)
    {
        super(input, execContext) ;
        graph = execContext.getActiveGraph() ;
		if (graph instanceof HBaseGraph) {
			output = buildChainOfIdBasedTriples(pattern, (HBaseGraph)graph, execContext);	        
			bindingMaterializer = new BindingMaterializer(graph);
		}
		else {
			// Create a chain of triple iterators.
			QueryIterator chain = getInput();
			for (Triple triple : pattern)
				chain = new QueryIterTriplePattern(chain, triple, execContext);
			output = chain;
		}
    }

	private QueryIterator buildChainOfIdBasedTriples(BasicPattern pattern, HBaseGraph graph, ExecutionContext execContext) {
		Map<Node, Node_Literal> node2NodeIdMap = new HashMap<Node, Node_Literal>();
		for (Triple triple : pattern) {
			addNodeToMap(node2NodeIdMap, triple.getSubject());
			addNodeToMap(node2NodeIdMap, triple.getPredicate());
			addNodeToMap(node2NodeIdMap, triple.getObject());
		}
		
		try {
			graph.mapMaterializedNodesToNodeIds(node2NodeIdMap);
		} catch (IOException e) {
			throw new JenaException(e.getMessage());
		}
		
		// Create a chain of triple iterators.
		QueryIterator chain = getInput() ;
		for (Triple triple : pattern){
			Node newSubject = mapNode(node2NodeIdMap, triple.getSubject());
			Node newPredicate = mapNode(node2NodeIdMap, triple.getPredicate());
			Node newObject = mapNode(node2NodeIdMap, triple.getObject());
			Triple idBasedTriple;
			//if one of the concrete elements did not have a matching Id, we don't create a triple for them
			if (newSubject!=null && newPredicate!=null && newObject!=null) { 
				if (triple instanceof FilteredTriple) {
					idBasedTriple = new FilteredTriple(newSubject, newPredicate, newObject,
							((FilteredTriple) triple).getSimpleFilter());
				} else {
					idBasedTriple = new Triple(newSubject, newPredicate, newObject);
				}

				chain = new QueryIterTriplePattern(chain, idBasedTriple, execContext);
			}
			else {
				//this subquery has missing elements so it won't return any results
				//we still insert it with dummy nodes so that we get empty bindings when we resolve it
				Triple dummyTriple = new Triple(Node.NULL, Node.NULL, Node.NULL);
				chain = new QueryIterTriplePattern(chain, dummyTriple, execContext);
			}
			
		}
		return chain;
	}

	private Node mapNode(Map<Node, Node_Literal> node2NodeIdMap, Node oldNode) {
		return oldNode.isConcrete() ? node2NodeIdMap.get(oldNode) : oldNode;
	}

	private void addNodeToMap(Map<Node, Node_Literal> node2NodeIdMap, Node node) {
		if (node.isConcrete()){
			node2NodeIdMap.put(node, null);
		}
	}

    @Override
    protected boolean hasNextBinding()
    {
        return output.hasNext() ;
    }

    @Override
    protected Binding moveToNextBinding()
    {
        Binding binding = output.nextBinding() ;
        if (binding instanceof BindingBase && graph instanceof HBaseGraph){
        	try {
				binding = bindingMaterializer.materialize(binding);
			} catch (IOException e) {
				return null;
			}
        }
        
        return binding;
    }

    @Override
    protected void closeSubIterator()
    {
        if ( output != null ){
            output.close() ;
            if (graph instanceof HBaseGraph){
            	bindingMaterializer.close();
            }
        }
        output = null ;
    }
    
    @Override
    protected void requestSubCancel()
    {
        if (output != null){
            output.cancel();
        }
    }

    @Override
    protected void details(IndentedWriter out, SerializationContext sCxt)
    {
        out.print(Utils.className(this)) ;
        out.println() ;
        out.incIndent() ;
        out.decIndent() ;
    }
}
