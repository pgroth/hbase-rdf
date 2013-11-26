package nl.vu.jena.graph;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
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
	
	//the bits which are encoded for the JOIN_POSITION byte 
	private static final int SPO_DIR = 0x00;
	private static final int OPS_DIR = 0x10;//5th bit 
	public static final int S = 0x08;//4th bit
	public static final int P = 0x04;//3rd bit
	public static final int O = 0x02;//2nd bit
	public static final int C = 0x01;//1st bit

	private ValueIdMapper valIdMapper;
	private IHBasePrefixMatchRetrieveOpsManager hbaseOpsManager;
	private List<ByteBuffer> varEncodings;
	private LinkedHashSet<String> varNames;
	private HashSet<String> joinVariableNames;
	private ArrayList<String> orderedJoinVariableNames;//the join key is built in the order SPO or OPS
	
	private HashMap<Byte, String> nonJoinVarMap;
	
	private short joinPatternId;
	private byte triplePatternId = 0;
	private byte currentNonJoinId;
	private LinkedHashSet<String> finalVarNames;
	
	public HBaseJoinHandler(IHBasePrefixMatchRetrieveOpsManager hbaseOpsManager, ValueIdMapper valIdMapper) {
		super();
		this.valIdMapper = valIdMapper;
		this.hbaseOpsManager = hbaseOpsManager;
	}
	
	public LinkedHashSet<String> processPattern(BasicPattern pattern, short joinPatternId) {
		List<Triple> tripleList = pattern.getList();
		varNames = new LinkedHashSet<String>();
		varEncodings = new ArrayList<ByteBuffer>();
		nonJoinVarMap = new HashMap<Byte, String>();
		joinVariableNames = new HashSet<String>();
		orderedJoinVariableNames = new ArrayList<String>();
		currentNonJoinId = 0;
		this.joinPatternId = joinPatternId;
		triplePatternId = 0;
		
		Iterator<Triple> it = tripleList.iterator();
		if (it.hasNext()){
			varEncodings.add(processTriple(it.next(), true));//adds null the first time
		}
		
		while (it.hasNext()){
			ByteBuffer varEncoding = processTriple(it.next(), false);
			varEncodings.add(varEncoding);
		}
		
		ByteBuffer firstTripleVarEncoding = reProcessFirstTriple(tripleList.get(0));
		varEncodings.set(0, firstTripleVarEncoding);
		
		varNames.removeAll(joinVariableNames);
		finalVarNames = new LinkedHashSet<String>(orderedJoinVariableNames.size()+varNames.size());
		finalVarNames.addAll(orderedJoinVariableNames);
		finalVarNames.addAll(varNames);	
		
		return finalVarNames;
	}
	
	
	private ByteBuffer processTriple(Triple triple, boolean isFirst) {
		byte joinPosition = 0x00;
		byte []varEncodingArray = new byte[7];
		ByteBuffer varEncoding;
		if (isFirst){
			varEncoding=null;
		}else{
			varEncoding = ByteBuffer.wrap(varEncodingArray);
			varEncoding.putShort(joinPatternId);
			varEncoding.put(triplePatternId++);
			varEncoding.put(joinPosition);
		}
		
		byte[] directionChange = {SPO_DIR};//TODO doesn't work for join keys with 3 variables in different positions (pretty uncommon)
		int expectedIndex = 0;
		if (processNode(triple.getSubject(), varEncoding, expectedIndex, directionChange)==true){
			joinPosition |= S;
			expectedIndex++;
		}
		if (processNode(triple.getPredicate(), varEncoding, expectedIndex, directionChange)==true){
			joinPosition |= P;
			expectedIndex++;
		}
		if (processNode(triple.getObject(), varEncoding, expectedIndex, directionChange)==true){
			joinPosition |= O;
			expectedIndex++;
		}
		
		if (!isFirst) {
			varEncodingArray[3] = (byte)(joinPosition|directionChange[0]);
			varEncoding.flip();
		}
		
		return varEncoding;
	}
	
	/**
	 * @param node
	 * @param varEncoding 
	 * @return true - if the currentNode is a join variable
	 * 			false - if it's not a join variable
	 */
	final private boolean processNode(Node node, ByteBuffer varEncoding, int expectedIndex, byte[] directionChange) {
		if (node.isVariable()){
			if (varNames.add(node.getName())==false){//variable appeared before so add it as a join var
				if (joinVariableNames.add(node.getName())==true){
					orderedJoinVariableNames.add(node.getName());
				}
				else{//if join variable added before
					String joinVar = orderedJoinVariableNames.get(expectedIndex);
					if (!node.getName().equals(joinVar)){
						directionChange[0]=OPS_DIR;
					}
				}
				return true;
			}
			else{
				createNonJoinId(node, varEncoding);
			}
		}
		
		return false;
	}
	
	private ByteBuffer reProcessFirstTriple(Triple triple) {
		byte joinPosition = 0x00;
		byte []varEncodingArray = new byte[7];
		ByteBuffer varEncoding = ByteBuffer.wrap(varEncodingArray);
		varEncoding.putShort(joinPatternId);
		varEncoding.put(triplePatternId++);
		varEncoding.put(joinPosition);
		
		Node subject = triple.getSubject();
		if (subject.isVariable()) {
			if (joinVariableNames.contains(subject.getName())) {
				joinPosition |= S;
			} else {
				createNonJoinId(subject, varEncoding);
			}
		}
		
		Node predicate = triple.getPredicate();
		if (predicate.isVariable()) {
			if (joinVariableNames.contains(predicate.getName())) {
				joinPosition |= P;
			} else {
				createNonJoinId(predicate, varEncoding);
			}
		}
		
		Node object = triple.getObject();
		if (object.isVariable()) {
			if (joinVariableNames.contains(object.getName())) {
				joinPosition |= O;
			} else {
				createNonJoinId(object, varEncoding);
			}
		}
		varEncodingArray[3] = joinPosition;
		varEncoding.flip();
		
		return varEncoding;
	}


	private void createNonJoinId(Node node, ByteBuffer varEncoding) {
		if (varEncoding != null) {
			varEncoding.put(currentNonJoinId);
			nonJoinVarMap.put(currentNonJoinId, node.getName());
			currentNonJoinId++;
		}
	}

	//ASSUMPTIONS
	// - processPattern is called before this function
 	// - the join results are retrieved in the order specified by the finalVarNames field
	public Iterator<ResultRow> getJoinResults(BasicPattern pattern) {
		try {
			List<Triple> triples = pattern.getList();

			ArrayList<Quad> quadPatterns = new ArrayList<Quad>(triples.size());
			for (Triple triple : triples) {
				quadPatterns.add(valIdMapper.getIdsFromTriple(triple));
			}

			ArrayList<ResultRow> results = hbaseOpsManager.joinTriplePatterns(quadPatterns, varEncodings, varNames);
			return results.iterator();
		} catch (IOException e) {
			e.printStackTrace();
			return NullIterator.instance();
		}
	}
}
