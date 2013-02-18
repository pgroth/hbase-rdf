package nl.vu.jena.sparql.engine.optimizer.reorder;

import nl.vu.jena.graph.FilteredTriple;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.PatternElements;
import com.hp.hpl.jena.sparql.sse.Item;
import com.hp.hpl.jena.sparql.sse.ItemList;

public class PatternTriple {
	public Item subject ;
    public Item predicate ;
    public Item object ;
    public boolean filtered = false;
    
    public static PatternTriple parse(Item pt)
    { 
        ItemList list = pt.getList();
        return new PatternTriple(list.get(0), list.get(1), list.get(2)) ; 
    }
    
    public PatternTriple(Item s, Item p, Item o)
    {
        set(normalize(s), normalize(p), normalize(o)) ;
    }
    
    public PatternTriple(Item s, Item p, Item o, boolean filtered)
    {
        set(normalize(s), normalize(p), normalize(o)) ;
        this.filtered = filtered;
    }
    
    private void set(Item s, Item p, Item o) 
    {
        subject =    s ;
        predicate =  p ;
        object =     o ;
    }
    
    public PatternTriple(Node s, Node p, Node o)
    {
        set(normalize(s),
            normalize(p),
            normalize(o)) ;
    }
    
    public PatternTriple(Triple triple)
    {
        this(triple.getSubject(),
             triple.getPredicate(),
             triple.getObject()) ;
        
        if (triple instanceof FilteredTriple){
        	this.filtered = true;
        }
    }
    
    @Override
    public String toString()
    { return subject+" "+predicate+" "+object ; }
    
    private static Item normalize(Item x)
    { return x != null ? x : PatternElements.ANY ; }
    
    private static Item normalize(Node x)
    { return x != null ? Item.createNode(x) : PatternElements.ANY ; }
}
