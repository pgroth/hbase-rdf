package nl.vu.jena.assembler;

import nl.vu.jena.sparql.engine.main.HBaseStageGenerator;
import nl.vu.jena.sparql.engine.optimizer.HBaseOptimize;
import nl.vu.jena.sparql.engine.optimizer.HBaseTransformFilterPlacement;

import com.hp.hpl.jena.query.ARQ;
import com.hp.hpl.jena.sparql.ARQConstants;
import com.hp.hpl.jena.sparql.engine.main.StageBuilder;

public class HBaseRewriter {
static {
	HBaseStageGenerator hbaseStageGenerator = new HBaseStageGenerator();
	StageBuilder.setGenerator(ARQ.getContext(), hbaseStageGenerator) ;
	
	ARQ.getContext().set(ARQConstants.sysOptimizerFactory, HBaseOptimize.hbaseOptimizationFactory);
	ARQ.getContext().set(ARQ.optFilterPlacement, new HBaseTransformFilterPlacement());
}
}
