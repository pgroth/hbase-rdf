package nl.vu.jena.sparql.engine.binding;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import nl.vu.jena.graph.HBaseGraph;

import org.openjena.atlas.lib.Closeable;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeId;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingBase;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingHashMap;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;
import com.hp.hpl.jena.sparql.engine.binding.BindingRoot;

public class BindingMaterializer implements Closeable {

	private Map<NodeId, Node> idToMaterializedNodesCache = new HashMap<NodeId, Node>();
	private Map<NodeId, Node> tempIdMap = new HashMap<NodeId, Node>();

	private Graph graph;

	public BindingMaterializer(Graph graph) {
		super();
		this.graph = graph;
	}
	
	public BindingMap materialize(BindingBase bindingBase) throws IOException{
		tempIdMap.clear();
    	
    	tempIdMap = buildIdMapToResolve(bindingBase);
    	        
    	((HBaseGraph)graph).mapNodeIdsToMaterializedNodes(tempIdMap);
    	
    	return updateBindingWithMaterializedNodes((BindingHashMap)bindingBase);	
	}
	
	private BindingMap updateBindingWithMaterializedNodes(BindingHashMap bindingHashMap) {
		
		
		//search first binding parent which does not map a NodeId
		Binding lastMaterialized = searchLastMaterializedBinding(bindingHashMap);	
		BindingMap materializedBinding = BindingFactory.create(lastMaterialized);
		
		Iterator<Var> it = bindingHashMap.vars();
		while (it.hasNext()){
			Var var = it.next();
			NodeId nodeId = (NodeId) bindingHashMap.get(var);
			Node materializedNode;
			
			if ((materializedNode=idToMaterializedNodesCache.get(nodeId))!=null){
				materializedBinding.add(var, materializedNode);
			}
			else{
				materializedBinding.add(var, tempIdMap.get(nodeId));
				idToMaterializedNodesCache.put(nodeId, tempIdMap.get(nodeId));
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
				if (!(b.get(first) instanceof NodeId)) {
					break;
				}
			}
			b = bHMap.getParent();
		}
		return b;
	}

	private Map<NodeId, Node> buildIdMapToResolve(Binding binding) {
		Iterator<Var> it = binding.vars();
		while (it.hasNext()){
			//build temp map for NodeIds not found in the cache
			NodeId nodeId = (NodeId) binding.get(it.next());
			if (!idToMaterializedNodesCache.containsKey(nodeId)){
				tempIdMap.put(nodeId, null);
			}
		}
		
		return tempIdMap;
	}

	@Override
	public void close() {
		idToMaterializedNodesCache.clear();
	}

}
