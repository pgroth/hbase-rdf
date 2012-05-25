package nl.vu.datalayer.hbase.sail;

import java.util.ArrayList;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

/**
 * The purpose of this visitor is list the contexts (Graphs) that each statement comes from.
 * <p>
 * The list of contexts is required so the system can look ahead to see if the next statement is in the same context.
 * if the next statement is in a different context it can close the GRAPH clause.
 * <p>
 * This helps with
 * <ol>
 * <li>Placing a Single Graph statement around the whole block.</li>
 * <li>Placing a single block of filters just inside the whole block.</li> 
 * </ol
 * 
 * @author Christian
 */
public class ContextListerVisitor extends QueryModelVisitorBase<QueryExpansionException>{

    private ArrayList<Var> contexts = new ArrayList<Var>();
    
    /**
     * Block outside creation of this visitor.
     * <p>
     * Use static getContexts(TupleExpr) method.
     */
    private ContextListerVisitor(){
    }
    
    @Override
    public void meet(StatementPattern sp) throws QueryExpansionException {
        //Record the context even if NULL.
    	System.out.println("FOUND CONTEXT STATEMENT PATTERN " + sp.getContextVar().toString());
        contexts.add(sp.getContextVar());
    }

    /**
     * Returns a list of the contexts found in this query or subquery.
     * <p>
     * @return An list of contexts. Warning this will probably include NULLs. 
     */
    private ArrayList<Var> getContexts(){
    	System.out.println("FOUND CONTEXTS");
        return contexts;
    }
    
    /**
     * Returns a list of the contexts found in this query or subquery.
     * <p>
     * Statements are checked in the normal visitor order. 
     * For every statement checked its context is added to the list even if null.
     * 
     * @param tupleExpr Query or subquery.
     * @return An list of contexts. Warning this will probably include NULLs. 
     * @throws QueryExpansionException 
     */
    public static ArrayList<Var> getContexts(TupleExpr tupleExpr) throws QueryExpansionException{
        ContextListerVisitor listener = new ContextListerVisitor();
        tupleExpr.visit(listener);
        return listener.getContexts();
    }
}
