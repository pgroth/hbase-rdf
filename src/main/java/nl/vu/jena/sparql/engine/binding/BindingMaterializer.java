package nl.vu.jena.sparql.engine.binding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import nl.vu.datalayer.hbase.id.Id;
import nl.vu.jena.graph.HBaseGraph;

import org.openjena.atlas.lib.Closeable;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_Literal;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingHashMap;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;
import com.hp.hpl.jena.sparql.engine.binding.BindingRoot;

public class BindingMaterializer implements Closeable {

	private Map<Node_Literal, Node> idToMaterializedNodesCache = new HashMap<Node_Literal, Node>();
	private Map<Node_Literal, Node> toResolveIdMap = new HashMap<Node_Literal, Node>();
	private List<Var> toUpdateVars = new ArrayList<Var>();

	private Graph graph;

	public BindingMaterializer(Graph graph) {
		super();
		this.graph = graph;
	}
	
	public Binding materialize(Binding inputBinding) throws IOException{
		toResolveIdMap.clear();
		toUpdateVars.clear();
    	
    	toResolveIdMap = buildIdMapToResolve(inputBinding);
    	        
    	((HBaseGraph)graph).mapNodeIdsToMaterializedNodes(toResolveIdMap);
    	
    	return updateBindingWithMaterializedNodes(inputBinding);	
	}
	
	private Binding updateBindingWithMaterializedNodes(Binding binding) {
		//search first binding parent which does not map a NodeId
		Binding lastMaterialized = searchLastMaterializedBinding(binding);	
		if (lastMaterialized == binding){
			return binding;
		}
		
		BindingMap materializedBinding = BindingFactory.create(lastMaterialized);	
		
		for (Var var : toUpdateVars) {
			Node_Literal nodeId = (Node_Literal) binding.get(var);
			Node materializedNode;
			
			if ((materializedNode=idToMaterializedNodesCache.get(nodeId))!=null){
				materializedBinding.add(var, materializedNode);
			}
			else{
				materializedBinding.add(var, toResolveIdMap.get(nodeId));
				idToMaterializedNodesCache.put(nodeId, toResolveIdMap.get(nodeId));
			}
		}
		return materializedBinding;
	}

	public Binding searchLastMaterializedBinding(Binding bindingStart) {
		Binding b = bindingStart;
		while(true){
			if (b instanceof BindingRoot){
				break;
			}
			
			BindingHashMap bHMap = (BindingHashMap)b;
			if (bHMap.vars1().hasNext()) {
				Var first = bHMap.vars1().next();
				Node firstElement = b.get(first);
				if (!(firstElement instanceof Node_Literal) || !(firstElement.getLiteralValue() instanceof Id)) {
					break;
				}
			}
			b = bHMap.getParent();
		}
		return b;
	}

	private Map<Node_Literal, Node> buildIdMapToResolve(Binding binding) {
		Iterator<Var> it = binding.vars();
		while (it.hasNext()){
			//build temp map for NodeIds not found in the cache
			Var currentVar = it.next();
			Node node = binding.get(currentVar);
			if (node instanceof Node_Literal && node.getLiteralValue() instanceof Id) {
				toUpdateVars.add(currentVar);
				Node_Literal nodeId = (Node_Literal) node;
				if (!idToMaterializedNodesCache.containsKey(nodeId)) {
					toResolveIdMap.put(nodeId, null);
				}
			}
		}
		
		return toResolveIdMap;
	}

	@Override
	public void close() {
		idToMaterializedNodesCache.clear();
	}

}
