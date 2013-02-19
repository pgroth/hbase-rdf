package nl.vu.jena.graph;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;

public class TripleBinder{

	public static Triple bindTriple(Triple pattern, Binding binding) {
		Node s = substitute(pattern.getSubject(), binding) ;
        Node p = substitute(pattern.getPredicate(), binding) ;
        Node o = substitute(pattern.getObject(), binding) ;
        
        if (pattern instanceof FilteredTriple){
        	return new FilteredTriple(s, p, o, ((FilteredTriple) pattern).getSimpleFilter());
        }
        else{
        	return new Triple(s, p, o);
        }
        
	}
	
	private static Node substitute(Node node, Binding binding)
    {
        if ( Var.isVar(node) )
        {
            Node x = binding.get(Var.alloc(node)) ;
            if ( x != null )
                return x ;
        }
        return node ;
    }

}
