package nl.vu.jena.graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;

import nl.vu.datalayer.hbase.operations.prefixmatch.IHBasePrefixMatchRetrieveOpsManager;
import nl.vu.datalayer.hbase.parameters.Quad;
import nl.vu.datalayer.hbase.parameters.ResultRow;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.util.iterator.NullIterator;

public class HBaseJoinHandler {

	private ValueIdMapper valIdMapper;
	private IHBasePrefixMatchRetrieveOpsManager hbaseOpsManager;
	private List<Byte> joinPositions;
	private LinkedHashSet<String> varNames;
	private HashSet<String> joinVariableNames;

	public HBaseJoinHandler(IHBasePrefixMatchRetrieveOpsManager hbaseOpsManager, ValueIdMapper valIdMapper) {
		super();
		this.valIdMapper = valIdMapper;
		this.hbaseOpsManager = hbaseOpsManager;
	}
	
	public LinkedHashSet<String> processPattern(BasicPattern pattern) {
		List<Triple> tripleList = pattern.getList();
		varNames = new LinkedHashSet<String>();
		joinPositions = new ArrayList<Byte>();
		
		for (Triple triple : tripleList) {
			byte joinPosition = processTriple(triple);
			joinPositions.add(joinPosition);
		}
		
		byte firstTripleJoinPosition = processTriple(tripleList.get(0));
		joinPositions.set(0, firstTripleJoinPosition);
		
		return varNames;
	}

	private byte processTriple(Triple triple) {
		byte joinPosition = 0x00;
		if (processNode(triple.getSubject())==true){
			joinPosition |= 0x08;
		}
		if (processNode(triple.getPredicate())==true){
			joinPosition |= 0x04;
		}
		if (processNode(triple.getObject())==true){
			joinPosition |= 0x02;
		}
		return joinPosition;
	}
	
	/**
	 * @param node
	 * @return true - if the currentNode is a join variable
	 * 			false - if it's not a join variable
	 */
	final private boolean processNode(Node node) {
		if (node.isVariable()){
			if (varNames.add(node.getName())==false){
				joinVariableNames.add(node.getName());
				return true;
			}
		}
		
		return false;
	}

	public Iterator<ResultRow> getJoinResults(BasicPattern pattern) {
		try {
			List<Triple> triples = pattern.getList();

			ArrayList<Quad> quadPatterns = new ArrayList<Quad>(triples.size());
			for (Triple triple : triples) {
				quadPatterns.add(valIdMapper.getIdsFromTriple(triple));
			}

			ArrayList<ResultRow> results = hbaseOpsManager.joinTriplePatterns(quadPatterns, joinPositions, varNames);
			return results.iterator();
		} catch (IOException e) {
			e.printStackTrace();
			return NullIterator.instance();
		}
	}
}
