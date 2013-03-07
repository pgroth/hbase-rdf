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
import com.hp.hpl.jena.graph.NodeId;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingBase;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIter1;
import com.hp.hpl.jena.sparql.serializer.SerializationContext;
import com.hp.hpl.jena.sparql.util.Utils;

public class QueryIterBlockTriples extends QueryIter1
{
    //TODO private BasicPattern pattern ; check if can be removed; only needed for printing
    private Graph graph ;
    private QueryIterator output ;
    private BindingMaterializer bindingMaterializer;
   
    
    public static QueryIterator create(QueryIterator input,
                                       BasicPattern pattern , 
                                       ExecutionContext execContext)
    {
        return new QueryIterBlockTriples(input, pattern, execContext) ;
    }
    
    private QueryIterBlockTriples(QueryIterator input,
                                    BasicPattern pattern , 
                                    ExecutionContext execContext)
    {
        super(input, execContext) ;
        //this.pattern = pattern ;
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
		Map<Node, NodeId> node2NodeIdMap = new HashMap<Node, NodeId>();
		for (Triple triple : pattern) {
			addNodeToMap(node2NodeIdMap, triple.getSubject());
			addNodeToMap(node2NodeIdMap, triple.getPredicate());
			addNodeToMap(node2NodeIdMap, triple.getObject());
		}
		
		try {
			graph.mapMaterializedNodesToNodeIds(node2NodeIdMap);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Create a chain of triple iterators.
		QueryIterator chain = getInput() ;
		for (Triple triple : pattern){
			Node newSubject = mapNode(node2NodeIdMap, triple.getSubject());
			Node newPredicate = mapNode(node2NodeIdMap, triple.getPredicate());
			Node newObject = mapNode(node2NodeIdMap, triple.getObject());
			Triple idBasedTriple;
			if (triple instanceof FilteredTriple){
				idBasedTriple = new FilteredTriple(newSubject, newPredicate, newObject, ((FilteredTriple)triple).getSimpleFilter());
			}
			else{
				idBasedTriple = new Triple(newSubject, newPredicate, newObject);
			}
			
		    chain = new QueryIterTriplePattern(chain, idBasedTriple, execContext) ;
		}
		return chain;
	}

	private Node mapNode(Map<Node, NodeId> node2NodeIdMap, Node oldNode) {
		return oldNode.isConcrete() ? node2NodeIdMap.get(oldNode) : oldNode;
	}

	private void addNodeToMap(Map<Node, NodeId> node2NodeIdMap, Node node) {
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
				binding = bindingMaterializer.materialize((BindingBase)binding);
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
            bindingMaterializer.close();
        }
        output = null ;
    }
    
    @Override
    protected void requestSubCancel()
    {
        if ( output != null )
            output.cancel();
    }

    @Override
    protected void details(IndentedWriter out, SerializationContext sCxt)
    {
        out.print(Utils.className(this)) ;
        out.println() ;
        out.incIndent() ;
        //FmtUtils.formatPattern(out, pattern, sCxt) ;
        out.decIndent() ;
    }
}
