package translate.redis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import com.hp.hpl.jena.sparql.expr.Expr;

import main.ShardedRedisTripleStore;

public class ReducePhaseJoin extends RedisJoinOP{

	public ReducePhaseJoin(RedisOP _lhs, RedisOP _rhs) {
		super(_lhs, _rhs);
		// TODO Auto-generated constructor stub
	}

	@Override
	public String mapLuaScript() {
		return lhs.mapLuaScript() + rhs.mapLuaScript();
	}
	
	protected String joinSignature(List<String> values, List<Integer> joinCols){
		StringBuilder result = new StringBuilder();
		for(Integer i:joinCols){
			result.append(values.get(i));
		}
		return result.toString();
	}
	
	List<Integer> joinColsLeft;
	List<Integer> joinColsRight;
	List<Integer> rightKeepColIdxs;
	List<String> joinedColNames;
	
	protected void computeJoinTable(QueryResult left, QueryResult right){

	}
	
	protected void computeJoinCols(QueryResult left, QueryResult right){
		joinColsLeft = new ArrayList<Integer>();
		joinColsRight = new ArrayList<Integer>();
		
		// get the list of join columns
		for(int l=0;l<left.columnNames.size();l++){
			for(int r=0;r<right.columnNames.size();r++){
				if(left.columnNames.get(l).equals(right.columnNames.get(r))){
					joinColsLeft.add(l);
					joinColsRight.add(r);
					break;
				}
			}
		}
		
		joinedColNames = new ArrayList<String>();
		rightKeepColIdxs = new ArrayList<Integer>();
		joinedColNames.addAll(left.columnNames);
		for(Integer r=0; r<right.columnNames.size(); r++){
			if(!joinColsRight.contains(r)){
				joinedColNames.add(right.columnNames.get(r));
				rightKeepColIdxs.add(r);
			}
		}
	}
	
	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
//		QueryResult right = rhs.reduce(patternStack);
//		QueryResult left = lhs.reduce(patternStack);
//
//		computeJoinCols(left, right);
//		
//		Map<String, List<String>> lhsJoinHash = new HashMap<String, List<String>>();
//		for(List<String> leftRow:left.rows){
//			lhsJoinHash.put(joinSignature(leftRow, joinColsLeft), leftRow);
//		}
//		
//		QueryResult result = new QueryResult(joinedColNames);
//		for(List<String> rightRow: right.rows){
//			List<String> leftRow = lhsJoinHash.get(joinSignature(rightRow, joinColsRight));
//			if(leftRow != null){
//				List<String> joinedRow = new ArrayList<String>();
//				joinedRow.addAll(leftRow);
//				for(Integer r:rightKeepColIdxs){
//					joinedRow.add(rightRow.get(r));
//				}
//				result.addRow(joinedRow);
//			}
//		}
//		
//		return result;
		
		QueryResult right = rhs.reduce(patternStack);
		QueryResult left = lhs.reduce(patternStack);

		computeJoinCols(left, right);
		
		Map<String, List<List<String>>> rhsJoinHash = new HashMap<String, List<List<String>>>();
		for(List<String> rightRow:right.rows){
			List<String> rightKeepRow = new ArrayList<String>();
			for(Integer r:rightKeepColIdxs){
				rightKeepRow.add(rightRow.get(r));
			}
			String joinSig = joinSignature(rightRow, joinColsRight);
			if(!rhsJoinHash.containsKey(joinSig)){
				List<List<String>> matches = new ArrayList<List<String>>();
				matches.add(rightKeepRow);
				rhsJoinHash.put(joinSig, matches);
			} else {
				rhsJoinHash.get(joinSig).add(rightKeepRow);
			}
		}
		
		QueryResult result = new QueryResult(joinedColNames);
		List<String> nullStrings = new ArrayList<String>();
		for(Integer r:rightKeepColIdxs){
			nullStrings.add("@");
		}
		for(List<String> leftRow: left.rows){
			List<String> joinedRow = new ArrayList<String>();
			joinedRow.addAll(leftRow);
			
			String joinSig = joinSignature(leftRow, joinColsLeft);
			List<List<String>> rhsMatches = rhsJoinHash.get(joinSig);
			if(rhsMatches != null){
				for(List<String> rightRow : rhsMatches)
				{
					List<String> joinedRowN = new ArrayList<String>();
					joinedRowN.addAll(joinedRow);
					joinedRowN.addAll(rightRow);
					result.addRow(joinedRowN);
				}
			} else {
				//joinedRow.addAll(nullStrings);
				//result.addRow(joinedRow);
			}
			
		}
		
		return result;
	}

	@Override
	public Boolean completeAfterMapPhase() {
		return false;
	}

	@Override
	public String toString(String indent) {
		return "ReducePhaseJoin {\n" +
				indent + "  left : " + lhs.toString(indent + "  ") + "\n" +
				indent + "  right: " + rhs.toString(indent + "  ") + "\n" +
				indent + "}";
	}

	
}
