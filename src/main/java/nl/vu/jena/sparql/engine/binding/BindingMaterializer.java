package nl.vu.jena.sparql.engine.binding;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import nl.vu.jena.graph.HBaseGraph;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeId;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;

public class BindingMaterializer {

	private Map<NodeId, Node> idToMaterializedNodesCache = new HashMap<NodeId, Node>();
	private Map<NodeId, Node> tempIdMap = new HashMap<NodeId, Node>();

	private Graph graph;

	public BindingMaterializer(Graph graph) {
		super();
		this.graph = graph;
	}
	
	public void materialize(BindingMap bindingMap){
		tempIdMap.clear();
    	
    	tempIdMap = buildIdMapToResolve(bindingMap);
    	        
    	try {
			((HBaseGraph)graph).mapNodeIdsToMaterializedNodes(tempIdMap);
		} catch (IOException e) {
			e.printStackTrace();
			return ;
		}
    	
    	updateBindingWithMaterializedNodes(bindingMap);	
	}
	
	private void updateBindingWithMaterializedNodes(BindingMap bindingMap) {
		Iterator<Var> it = bindingMap.vars();
		
		while (it.hasNext()){
			Var var = it.next();
			NodeId nodeId = (NodeId) bindingMap.get(var);
			Node concreteNode;
			
			if ((concreteNode=idToMaterializedNodesCache.get(nodeId))!=null){
				bindingMap.add(var, concreteNode);
			}
			else{
				bindingMap.add(var, tempIdMap.get(nodeId));
			}
		}
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

}
