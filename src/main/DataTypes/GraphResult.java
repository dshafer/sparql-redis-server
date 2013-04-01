package main.DataTypes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class GraphResult implements Iterator<List<String>> {
	public List<String> columnNames;
	public List<List<String>> values;
	
	public GraphResult(){
		iteratorPtr = 0;
		columnNames = new ArrayList<String>();
		values = new ArrayList<List<String>>();
	}
	
	public GraphResult(List<String> redisResult) {
		iteratorPtr = 0;
		
	}
	
	public String toRedisList(){
		throw new UnsupportedOperationException();
	}

	private int iteratorPtr;
	@Override
	public boolean hasNext() {
		return values.size() > iteratorPtr;
	}

	@Override
	public List<String> next() {
		List<String> result = values.get(iteratorPtr);
		iteratorPtr++;
		return result;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}
}
