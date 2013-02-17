package nl.vu.jena.sparql.engine.optimizer;

import static com.hp.hpl.jena.sparql.engine.optimizer.reorder.PatternElements.TERM;
import static com.hp.hpl.jena.sparql.engine.optimizer.reorder.PatternElements.VAR;

import com.hp.hpl.jena.sparql.engine.optimizer.reorder.PatternTriple;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.ReorderTransformationBase;
import com.hp.hpl.jena.sparql.graph.NodeConst;
import com.hp.hpl.jena.sparql.sse.Item;

public class ReorderHeuristics extends ReorderTransformationBase {

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
        
        matcher.addPattern(new Pattern(1,   TERM, TERM, TERM)) ;     // SPO - built-in - not needed a s a rule
        matcher.addPattern(new Pattern(2,   TERM, type, TERM)) ;
        // Numbers choosen as an approximation for a graph of 100 triples
        matcher.addPattern(new Pattern(3,   TERM, VAR, TERM)) ;    // S?O
        
        matcher.addPattern(new Pattern(4,   VAR,  TERM, TERM)) ;    // ?PO
        matcher.addPattern(new Pattern(5,   TERM, TERM, VAR)) ;     // SP?
        
        matcher.addPattern(new Pattern(8,   VAR,  type, TERM)) ;   // ? type O -- worse than ?PO
        
        matcher.addPattern(new Pattern(10,  VAR,  VAR,  TERM)) ;    // ??O
        matcher.addPattern(new Pattern(20,  TERM, VAR,  VAR)) ;     // S??
        matcher.addPattern(new Pattern(30,  VAR,  TERM, VAR)) ;     // ?P?

        matcher.addPattern(new Pattern(MultiTermSampleSize, VAR,  VAR,  VAR)) ;     // ???
    }
    
    @Override
    public double weight(PatternTriple pt)
    {
        return matcher.match(pt) ;
    }

}
