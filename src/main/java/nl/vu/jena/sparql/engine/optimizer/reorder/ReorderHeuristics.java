package nl.vu.jena.sparql.engine.optimizer.reorder;

import static com.hp.hpl.jena.sparql.engine.optimizer.reorder.PatternElements.TERM;
import static com.hp.hpl.jena.sparql.engine.optimizer.reorder.PatternElements.VAR;

import java.util.List;

import org.openjena.atlas.iterator.Iter;
import org.openjena.atlas.iterator.Transform;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.ARQException;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.PatternElements;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.ReorderLib;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.ReorderProc;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.ReorderProcIndexes;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import com.hp.hpl.jena.sparql.graph.NodeConst;
import com.hp.hpl.jena.sparql.sse.Item;

public class ReorderHeuristics implements ReorderTransformation {
	
	public static final boolean FILTERED = true;

	public ReorderHeuristics() {}
    // Fixed scheme for when we have no stats.
    // It chooses a triple pattern by order of preference.
    
    private static Item type = Item.createNode(NodeConst.nodeRDFType) ;
    
    /** The number of triples used for the base scale */
    public static int MultiTermSampleSize = 100 ; 

    /** Maximum value for a match involving two terms. */
    public static int MultiTermMax = 9 ; 
    
    public final static StatsMatcher matcher ;
    static {
        matcher = new StatsMatcher() ;
        
        matcher.addPattern(new Pattern(1,   TERM, TERM, TERM, FILTERED)) ;     // SPO - built-in - not needed a s a rule
        matcher.addPattern(new Pattern(2,   TERM, TERM, TERM)) ;     // SPO - built-in - not needed a s a rule
        
        matcher.addPattern(new Pattern(3,   TERM, type, TERM, FILTERED)) ;
        matcher.addPattern(new Pattern(4,   TERM, type, TERM)) ;
        
        // Numbers choosen as an approximation for a graph of 100 triples
        matcher.addPattern(new Pattern(5,   TERM, VAR, TERM, FILTERED)) ;    // S?O
        matcher.addPattern(new Pattern(6,   TERM, VAR, TERM)) ;    // S?O
        
        matcher.addPattern(new Pattern(7,   VAR,  TERM, TERM, FILTERED)) ;    // ?PO
        matcher.addPattern(new Pattern(8,   VAR,  TERM, TERM)) ;    // ?PO
        
        matcher.addPattern(new Pattern(9,   TERM, TERM, VAR, FILTERED)) ;     // SP?
        matcher.addPattern(new Pattern(10,   TERM, TERM, VAR)) ;     // SP?
        
        matcher.addPattern(new Pattern(11,   VAR,  type, TERM, FILTERED)) ;
        matcher.addPattern(new Pattern(12,   VAR,  type, TERM)) ;   // ? type O -- worse than ?PO
        
        
        matcher.addPattern(new Pattern(18,  VAR,  VAR,  TERM, FILTERED)) ;    // ??O
        matcher.addPattern(new Pattern(20,  VAR,  VAR,  TERM)) ;    // ??O
        
        matcher.addPattern(new Pattern(28,  TERM, VAR,  VAR, FILTERED)) ;     // S??
        matcher.addPattern(new Pattern(30,  TERM, VAR,  VAR)) ;     // S??
        
        matcher.addPattern(new Pattern(38,  VAR,  TERM, VAR, FILTERED)) ;     // ?P?
        matcher.addPattern(new Pattern(40,  VAR,  TERM, VAR)) ;     // ?P?

        matcher.addPattern(new Pattern(MultiTermSampleSize, VAR,  VAR,  VAR)) ;     // ???
    }
    
   
    public double weight(PatternTriple pt)
    {
        return matcher.match(pt) ;
    }
    
    @Override
	public BasicPattern reorder(BasicPattern pattern) {
		return reorderIndexes(pattern).reorder(pattern) ;
	}

	@Override
	public ReorderProc reorderIndexes(BasicPattern pattern) {
		if (pattern.size() <= 1 )
            return ReorderLib.identityProc() ;
        
        List<Triple> triples = pattern.getList() ;

        // Could merge into the conversion step to do the rewrite WRT a Binding.
        // Or done here as a second pass mutate of PatternTriples

        // Convert to a mutable form (that allows things like "TERM")
        List<PatternTriple> components = Iter.toList(Iter.map(triples, convert)) ;

        // Allow subclasses to get in (e.g. static reordering).
        ReorderProc proc = reorder(triples, components) ;
        return proc ;
	}
	
	private ReorderProc reorder(List<Triple> triples, List<PatternTriple> components)
    {
        int N = components.size() ;
        int numReorder = N ;        // Maybe choose 4, say, and copy over the rest.
        int indexes[] = new int[N] ;
        
        int idx = 0 ;
        for ( ; idx < numReorder ; idx++ )
        {
            int j = chooseNext(components) ;
            if ( j < 0 )
                break ;
            Triple triple = triples.get(j) ;
            indexes[idx] = j ;
            update(triple, components) ;
            components.set(j, null) ;
        }
        
        // Copy over the remainder (if any) 
        for ( int i = 0 ; i < components.size() ; i++ )
        {
            if ( components.get(i) != null )
                indexes[idx++] = i ;
        }
        if ( triples.size() != idx )
            throw new ARQException(String.format("Inconsistency: number of triples (%d) does not equal to number of indexes processed (%d)", triples.size(), idx)) ;
        
        ReorderProc proc = new ReorderProcIndexes(indexes) ;
        
        return proc ;
    }
	
	/** Update components to note any variables from triple */
	private static void update(Triple triple, List<PatternTriple> components)
    {
        for ( PatternTriple elt : components )
            if ( elt != null ){
            	update(triple.getSubject(), elt) ;
                update(triple.getPredicate(), elt) ;
                update(triple.getObject(), elt) ;
            }
    }

    private static void update(Node node, PatternTriple elt)
    {
        if ( Var.isVar(node) )
        {
            if ( node.equals(elt.subject.getNode()) )
                elt.subject = PatternElements.TERM ;
            if ( node.equals(elt.predicate.getNode()) )
                elt.predicate = PatternElements.TERM ;
            if ( node.equals(elt.object.getNode()) )
                elt.object = PatternElements.TERM ;
        }
    }
	
	
	/** Return index of next pattern triple */
    protected int chooseNext(List<PatternTriple> pTriples)
    {    
        int idx = processPTriples(pTriples, null) ;   
       
        return idx ;
    }
    
    /** Return the index of the first, least triple; optionally accumulate all indexes of the same least weight */ 
    private int processPTriples(List<PatternTriple> pTriples, List<Integer> results)
    {
        double min = Double.MAX_VALUE ;     // Current minimum
        int N = pTriples.size() ;
        int idx = -1 ;
        
        for ( int i = 0 ; i < N ; i++ )
        {
            PatternTriple pt = pTriples.get(i) ;
            if ( pt == null )
                continue ;
            double x = weight(pt) ;
            
            if ( x == min )
            {
                if ( results != null ) results.add(i) ;
                continue ;
            }
            
            if ( x < min )
            {
                min = x ;
                idx = i ;
                if ( results != null )
                {
                    results.clear() ;
                    results.add(i) ;
                }
            }
        }
        return idx ;
    }
    
 // Triples to TriplePatterns.
    private static Transform<Triple, PatternTriple> convert = new Transform<Triple, PatternTriple>(){
        @Override
        public PatternTriple convert(Triple triple)
        {
            return new PatternTriple(triple) ;
        }} ;

}