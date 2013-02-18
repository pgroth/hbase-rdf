package nl.vu.jena.graph;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.expr.Expr;

public class FilteredTriple extends Triple {
	
	private Expr simpleFilter;

	public FilteredTriple(Node s, Node p, Node o, Expr simpleFilter) {
		super(s, p, o);
		this.simpleFilter = simpleFilter;
	}
	
	public FilteredTriple(Triple t, Expr simpleFilter){
		super(t.getSubject(), t.getPredicate(), t.getMatchObject());
		this.simpleFilter = simpleFilter;
	}

	public Expr getSimpleFilter() {
		return simpleFilter;
	}

	public void setSimpleFilter(Expr simpleFilter) {
		this.simpleFilter = simpleFilter;
	}
	
}
