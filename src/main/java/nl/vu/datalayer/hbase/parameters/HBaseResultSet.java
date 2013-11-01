package nl.vu.datalayer.hbase.parameters;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import nl.vu.datalayer.hbase.id.Id;

public class HBaseResultSet extends AbstractCollection<ResultRow> {

	private ArrayList<String> columnsNames;
	private HashMap<String, ResultColumn> resultColumns;
	
	public HBaseResultSet() {
		super();
		columnsNames = new ArrayList<String>();
		resultColumns = new HashMap<String, ResultColumn>();
	}
	
	@Override
	public Iterator<ResultRow> iterator() {
		return new ResultIterator();
	}
	@Override
	public int size() {
		if (resultColumns.size()==0)
			return 0;
		
		ResultColumn aColumn = resultColumns.values().iterator().next();
		return aColumn.getResults().size();
	}
	
	public boolean addToColumn(String colName, Id id){
		ResultColumn resultCol = resultColumns.get(colName);
		if (resultCol==null){
			resultCol = new ResultColumn(colName.getBytes());
			resultColumns.put(colName, resultCol);
		}
		
		resultCol.addResult(id);
		/*if (isJoinKey){
			columnsNames.add(colName);
		}*/
		
		return true;
	}
	
	@Override
	public boolean add(ResultRow e) {
		// TODO Auto-generated method stub
		return super.add(e);
	}
	
	private class ResultIterator implements Iterator<ResultRow>{
		
		ArrayList<Iterator<Id>> iterators = new ArrayList<Iterator<Id>>();
		
		public ResultIterator() {
			super();
			for (String joinCol : columnsNames) {
				Iterator<Id> it = resultColumns.get(joinCol).getResults().iterator();
				iterators.add(it);
			}
		}

		@Override
		public boolean hasNext() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public ResultRow next() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	

}
