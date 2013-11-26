package nl.vu.jena.sparql.engine.iterator;

import java.util.HashSet;
import java.util.Set;

import nl.vu.jena.graph.TripleBinder;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.ARQInternalErrorException;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIter;
import com.hp.hpl.jena.util.iterator.ClosableIterator;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NiceIterator;

public class TripleMapper extends QueryIter implements Joinable
{
    private Node s ;
    private Node p ;
    private Node o ;
    private Binding binding ;
    private ClosableIterator<Triple> graphIter ;
    private Binding slot = null ;
    private boolean finished = false ;
    private volatile boolean cancelled = false ;
    
    private HashSet<String> varNames;

    public TripleMapper(Binding binding, Triple pattern, ExecutionContext cxt)
    {
        super(cxt) ;
        
        this.binding = binding ;
        Graph graph = cxt.getActiveGraph() ;
        
        varNames = new HashSet<String>();
        Triple bindedPattern = TripleBinder.bindTriple(pattern, binding);
        this.s = bindedPattern.getSubject();
        if (Var.isVar(s))
            varNames.add(s.getName());
        this.p = bindedPattern.getPredicate();
        if (Var.isVar(p))
            varNames.add(p.getName());
        this.o = bindedPattern.getObject();
        if (Var.isVar(o))
            varNames.add(o.getName());
        
        ExtendedIterator<Triple> iter = graph.find(bindedPattern) ;
        
        this.graphIter = iter ;
    }
    
    @Override
    protected boolean hasNextBinding()
    {
        if ( finished ) return false ;
        if ( slot != null ) return true ;
        if ( cancelled )
        {
            graphIter.close() ;
            finished = true ;
            return false ;
        }

        while(graphIter.hasNext() && slot == null )
        {
            Triple t = graphIter.next() ;
            slot = mapper(t) ;
        }
        if ( slot == null )
            finished = true ;
        return slot != null ;
    }

    @Override
    protected Binding moveToNextBinding()
    {
        if ( ! hasNextBinding() ) 
            throw new ARQInternalErrorException() ;
        Binding r = slot ;
        slot = null ;
        return r ;
    }
    
    private Binding mapper(Triple r)
    {
        BindingMap results = BindingFactory.create(binding) ;

        if ( ! insert(s, r.getSubject(), results) )
            return null ; 
        if ( ! insert(p, r.getPredicate(), results) )
            return null ;
        if ( ! insert(o, r.getObject(), results) )
            return null ;
        return results ;
    }

    private static boolean insert(Node inputNode, Node outputNode, BindingMap results)
    {
        if ( ! Var.isVar(inputNode) )
            return true ;
        
        Var v = Var.alloc(inputNode) ;
        Node x = results.get(v) ;
        if ( x != null )
            return outputNode.equals(x) ;
        
        results.add(v, outputNode) ;
        return true ;
    }

    @Override
	public Set<String> getVarNames() {
		return varNames;
	}

	@Override
    protected void closeIterator()
    {
        if ( graphIter != null )
            NiceIterator.close(graphIter) ;
        graphIter = null ;
    }
    
    @Override
    protected void requestCancel()
    {
        // The QueryIteratorBase machinary will do the real work.
        cancelled = true ;
    }
}
