package nl.vu.jena.sparql.engine.hsp;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import org.junit.Before;

import org.junit.Test;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.BasicPattern;

public class HSPHeuristicsTest {
	
	private Node xNode;
	private Node yNode;
	
	@Before
	public void beforeTest(){
		xNode = Node.createVariable("x");
		yNode = Node.createVariable("y");
	}

	@Test
	public void testHeuristic3_DistintPositions(){
		BasicPattern pattern = new BasicPattern();
		Triple t1 = Triple.create(xNode, Node.createURI("http://bla1"), Node.createVariable("u"));
		Triple t2 = Triple.create(xNode, Node.createURI("http://bla2"), Node.createVariable("z"));
		Triple t3 = Triple.create(xNode, Node.createURI("http://bla3"), yNode);
		Triple t4 = Triple.create(yNode, Node.createURI("http://bla4"), Node.createVariable("u1"));
		Triple t5 = Triple.create(yNode, Node.createURI("http://bla5"), Node.createVariable("z1"));
		
		pattern.add(t1);
		pattern.add(t2);
		pattern.add(t3);
		pattern.add(t4);
		pattern.add(t5);
		
		WeightedGraphNode node1 = new WeightedGraphNode("x", 3);
		WeightedGraphNode node2 = new WeightedGraphNode("y", 3);
		
		ArrayList<HashSet<WeightedGraphNode>> maximumISets = new ArrayList<HashSet<WeightedGraphNode>>();
		HashSet<WeightedGraphNode> set1 = new HashSet<WeightedGraphNode>();
		set1.add(node1);
		maximumISets.add(set1);
		
		HashSet<WeightedGraphNode> set2 = new HashSet<WeightedGraphNode>();
		set2.add(node2);
		maximumISets.add(set2);
		
		maximumISets = HSPHeuristics.applyHeuristicH3(pattern, maximumISets);
		
		assertTrue(maximumISets.size()==1);
		set1 = maximumISets.get(0);
		assertTrue(set1.iterator().next().getId().equals("y"));
		
	}
	
	@Test
	public void testHeuristic3_SamePos(){
		BasicPattern pattern = new BasicPattern();
		Triple t1 = Triple.create(xNode, Node.createURI("http://bla1"), Node.createVariable("u"));
		Triple t2 = Triple.create(xNode, Node.createURI("http://bla2"), Node.createVariable("z"));
		Triple t3 = Triple.create(xNode, Node.createURI("http://bla3"), yNode);
		Triple t4 = Triple.create(Node.createVariable("u1"), Node.createURI("http://bla4"), yNode);
		Triple t5 = Triple.create(Node.createVariable("z1"), Node.createURI("http://bla5"), yNode);
		
		pattern.add(t1);
		pattern.add(t2);
		pattern.add(t3);
		pattern.add(t4);
		pattern.add(t5);
		
		WeightedGraphNode node1 = new WeightedGraphNode("x", 3);
		WeightedGraphNode node2 = new WeightedGraphNode("y", 3);
		
		ArrayList<HashSet<WeightedGraphNode>> maximumISets = new ArrayList<HashSet<WeightedGraphNode>>();
		HashSet<WeightedGraphNode> set1 = new HashSet<WeightedGraphNode>();
		set1.add(node1);
		maximumISets.add(set1);
		
		HashSet<WeightedGraphNode> set2 = new HashSet<WeightedGraphNode>();
		set2.add(node2);
		maximumISets.add(set2);
		
		maximumISets = HSPHeuristics.applyHeuristicH3(pattern, maximumISets);
		
		assertTrue(maximumISets.size()==1);
		set1 = maximumISets.get(0);
		assertTrue(set1.iterator().next().getId().equals("y"));
	}
	
	@Test
	public void testHeuristic3_SamePos2(){
		BasicPattern pattern = new BasicPattern();
		Triple t1 = Triple.create(Node.createURI("http://bla1"), xNode, Node.createVariable("u"));
		Triple t2 = Triple.create(xNode, Node.createURI("http://bla2"), Node.createVariable("z"));
		Triple t3 = Triple.create(xNode, yNode, Node.createURI("http://bla3"));
		Triple t4 = Triple.create(Node.createVariable("u1"), Node.createURI("http://bla4"), yNode);
		Triple t5 = Triple.create(Node.createVariable("z1"), Node.createURI("http://bla5"), yNode);
		
		pattern.add(t1);
		pattern.add(t2);
		pattern.add(t3);
		pattern.add(t4);
		pattern.add(t5);
		
		WeightedGraphNode node1 = new WeightedGraphNode("x", 3);
		WeightedGraphNode node2 = new WeightedGraphNode("y", 3);
		
		ArrayList<HashSet<WeightedGraphNode>> maximumISets = new ArrayList<HashSet<WeightedGraphNode>>();
		HashSet<WeightedGraphNode> set1 = new HashSet<WeightedGraphNode>();
		set1.add(node1);
		maximumISets.add(set1);
		
		HashSet<WeightedGraphNode> set2 = new HashSet<WeightedGraphNode>();
		set2.add(node2);
		maximumISets.add(set2);
		
		maximumISets = HSPHeuristics.applyHeuristicH3(pattern, maximumISets);
		
		assertTrue(maximumISets.size()==1);
		set1 = maximumISets.get(0);
		assertTrue(set1.iterator().next().getId().equals("y"));
	}
	
	@Test
	public void testHeuristic3_SamePos3(){
		BasicPattern pattern = new BasicPattern();
		Triple t1 = Triple.create(Node.createURI("http://bla1"), Node.createVariable("u"), xNode);
		Triple t2 = Triple.create(Node.createURI("http://bla2"), xNode, Node.createVariable("z"));
		Triple t3 = Triple.create(xNode, yNode, Node.createURI("http://bla3"));
		Triple t4 = Triple.create(Node.createVariable("u1"), Node.createURI("http://bla4"), yNode);
		Triple t5 = Triple.create(yNode, Node.createVariable("z1"), Node.createURI("http://bla5"));
		
		pattern.add(t1);
		pattern.add(t2);
		pattern.add(t3);
		pattern.add(t4);
		pattern.add(t5);
		
		WeightedGraphNode node1 = new WeightedGraphNode("x", 3);
		WeightedGraphNode node2 = new WeightedGraphNode("y", 3);
		
		ArrayList<HashSet<WeightedGraphNode>> maximumISets = new ArrayList<HashSet<WeightedGraphNode>>();
		HashSet<WeightedGraphNode> set1 = new HashSet<WeightedGraphNode>();
		set1.add(node1);
		maximumISets.add(set1);
		
		HashSet<WeightedGraphNode> set2 = new HashSet<WeightedGraphNode>();
		set2.add(node2);
		maximumISets.add(set2);
		
		maximumISets = HSPHeuristics.applyHeuristicH3(pattern, maximumISets);
		
		assertTrue(maximumISets.size()==2);
		
	}
	
	

}
