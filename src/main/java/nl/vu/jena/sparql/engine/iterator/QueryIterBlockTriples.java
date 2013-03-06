package nl.vu.jena.sparql.engine.iterator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import nl.vu.jena.graph.HBaseGraph;
import nl.vu.jena.sparql.engine.binding.BindingMaterializer;

import org.openjena.atlas.io.IndentedWriter;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeId;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIter1;
import com.hp.hpl.jena.sparql.serializer.SerializationContext;
import com.hp.hpl.jena.sparql.util.FmtUtils;
import com.hp.hpl.jena.sparql.util.Utils;

public class QueryIterBlockTriples extends QueryIter1
{
    private BasicPattern pattern ;
    private Graph graph ;
    private QueryIterator output ;
    private BindingMaterializer bindingMaterializer;
   
    
    public static QueryIterator create(QueryIterator input,
                                       BasicPattern pattern , 
                                       ExecutionContext execContext)
    {
        return new QueryIterBlockTriples(input, pattern, execContext) ;
    }
    
    private QueryIterBlockTriples(QueryIterator input,
                                    BasicPattern pattern , 
                                    ExecutionContext execContext)
    {
        super(input, execContext) ;
        this.pattern = pattern ;
        graph = execContext.getActiveGraph() ;
        //TODO map Binded triple elements to Ids
        
        // Create a chain of triple iterators.
        QueryIterator chain = getInput() ;
        for (Triple triple : pattern)
            chain = new QueryIterTriplePattern(chain, triple, execContext) ;
        output = chain ;
        bindingMaterializer = new BindingMaterializer(graph);
        
    }

    @Override
    protected boolean hasNextBinding()
    {
        return output.hasNext() ;
    }

    @Override
    protected Binding moveToNextBinding()
    {
        Binding binding = output.nextBinding() ;
        if (binding instanceof BindingMap && graph instanceof HBaseGraph){
        	bindingMaterializer.materialize((BindingMap)binding);
        }
        
        return binding;
    }

    @Override
    protected void closeSubIterator()
    {
        if ( output != null )
            output.close() ;
        output = null ;
    }
    
    @Override
    protected void requestSubCancel()
    {
        if ( output != null )
            output.cancel();
    }

    @Override
    protected void details(IndentedWriter out, SerializationContext sCxt)
    {
        out.print(Utils.className(this)) ;
        out.println() ;
        out.incIndent() ;
        FmtUtils.formatPattern(out, pattern, sCxt) ;
        out.decIndent() ;
    }
}
