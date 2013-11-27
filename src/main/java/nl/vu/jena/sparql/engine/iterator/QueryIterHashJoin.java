package nl.vu.jena.sparql.engine.iterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import nl.vu.datalayer.hbase.id.Id;

import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIter2;

public class QueryIterHashJoin extends QueryIter2 implements Joinable{

	private ArrayList<Var> joinKeyVariables;
	private HashMap<Integer, Binding> leftHashTable;
	private Binding nextBinding;
	private HashSet<String> varNames;
	
	public QueryIterHashJoin(QueryIterator left, QueryIterator right, 
			ArrayList<String> joinKeyVariables, ExecutionContext execCxt) {
		super(left, right, execCxt);	
		buildVarNames((Joinable)left, (Joinable)right);
		
		for (String joinVarName : joinKeyVariables) {
			this.joinKeyVariables.add(Var.alloc(joinVarName));
		}
		
		this.leftHashTable = new HashMap<Integer, Binding>();
		buildHashTable(left);
	}

	private void buildVarNames(Joinable left, Joinable right) {
		varNames = new HashSet<String>();
		for (String string : left.getVarNames()) {
			varNames.add(string);
		}
		for (String string : right.getVarNames()) {
			varNames.add(string);
		}
	}
	
	private void buildHashTable(QueryIterator left){
		while (left.hasNext()){
			Binding binding = left.next();
			int hash = computeJoinHash(binding);
			leftHashTable.put(hash, binding);
		}
	}

	@Override
	public Set<String> getVarNames() {
		return varNames;
	}

	@Override
	protected void requestSubCancel() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void closeSubIterator() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected boolean hasNextBinding() {		
		if (leftHashTable.size()==0){
			return false;
		}
		
		BindingMap newBinding = null;	
	
		while (getRight().hasNext()){
			Binding right = getRight().next();
			int rightHash = computeJoinHash(right);
			Binding left = leftHashTable.get(rightHash);
			if (left!=null){
				newBinding = BindingFactory.create(left) ;
		        for (Iterator<Var> vIter = right.vars() ; vIter.hasNext() ;)
		        {
		            Var v = vIter.next();
		            Node n = right.get(v) ;
		            if (!left.contains(v)){
		                newBinding.add(v, n) ;
		            }
		        }
		        break;
			}
		}
		
		if (newBinding==null){
			return false;
		}
		else{
			nextBinding = newBinding;
			return true;
		}
	}

	@Override
	protected Binding moveToNextBinding() {
		return nextBinding;
	}
	
	private int computeJoinHash(Binding next) {
		byte []joinKey = new byte[0];
	
		for (Var joinVar : joinKeyVariables) {
			Id id = (Id)next.get(joinVar).getLiteralValue();
			joinKey = Bytes.add(joinKey, id.getBytes());
		}
			
		return Bytes.hashCode(joinKey);
	}

	
}
