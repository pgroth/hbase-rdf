package nl.vu.jena.sparql.engine.iterator;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterRepeatApply;

public class QueryIterTriplePattern extends QueryIterRepeatApply {

private final Triple pattern ;
    
    public QueryIterTriplePattern( QueryIterator input,
                                   Triple pattern , 
                                   ExecutionContext cxt)
    {
        super(input, cxt) ;
        this.pattern = pattern ;
    }

    @Override
    protected QueryIterator nextStage(Binding binding)
    {
        return new TripleMapper(binding, pattern, getExecContext()) ;
    }
    
    static int countMapper = 0 ; 
    

}
