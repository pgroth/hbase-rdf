package nl.vu.jena.sparql.engine.main;

import java.util.List;
import java.util.Stack;

import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.main.OpExecutor;
import com.hp.hpl.jena.sparql.engine.main.OpExecutorFactory;

public class HBaseOpExecutor extends OpExecutor {
	
	public static final OpExecutorFactory hbaseOpExecFactory = new OpExecutorFactory(){
        @Override
        public OpExecutor create(ExecutionContext execCxt)
        {
            return new HBaseOpExecutor(execCxt) ;
    }};

	protected HBaseOpExecutor(ExecutionContext execCxt) {
		super(execCxt);
	}

	@Override
	protected QueryIterator execute(OpProject opProject, QueryIterator input) {
		Stack<List<Var>> projectionVars = (Stack<List<Var>>)execCxt.getContext().get(HBaseSymbols.PROJECTION_VARS);
		projectionVars.push(opProject.getVars());
		QueryIterator ret = super.execute(opProject, input);
		projectionVars.pop();
		
		return ret;
	}
	
	

	

}
