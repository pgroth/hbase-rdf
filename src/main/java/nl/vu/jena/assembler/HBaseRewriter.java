package nl.vu.jena.assembler;

import java.util.List;
import java.util.Stack;
import java.util.concurrent.Executors;

import nl.vu.jena.sparql.engine.main.HBaseOpExecutor;
import nl.vu.jena.sparql.engine.main.HBaseStageGenerator;
import nl.vu.jena.sparql.engine.main.HBaseSymbols;
import nl.vu.jena.sparql.engine.optimizer.HBaseOptimize;
import nl.vu.jena.sparql.engine.optimizer.HBaseTransformFilterPlacement;

import com.hp.hpl.jena.query.ARQ;
import com.hp.hpl.jena.sparql.ARQConstants;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.main.QC;
import com.hp.hpl.jena.sparql.engine.main.StageBuilder;

public class HBaseRewriter {
static {
	HBaseStageGenerator hbaseStageGenerator = new HBaseStageGenerator();
	StageBuilder.setGenerator(ARQ.getContext(), hbaseStageGenerator) ;
	
	ARQ.getContext().set(ARQConstants.sysOptimizerFactory, HBaseOptimize.hbaseOptimizationFactory);
	ARQ.getContext().set(ARQ.optFilterPlacement, new HBaseTransformFilterPlacement());
	
	ARQ.getContext().set(HBaseSymbols.EXECUTOR, Executors.newFixedThreadPool(2*Runtime.getRuntime().availableProcessors()));
	ARQ.getContext().set(HBaseSymbols.PROJECTION_VARS, new Stack<List<Var>>());
	
	QC.setFactory(ARQ.getContext(), HBaseOpExecutor.hbaseOpExecFactory);
	
}
}
