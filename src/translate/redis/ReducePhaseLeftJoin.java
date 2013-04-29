package translate.redis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

public class ReducePhaseLeftJoin extends ReducePhaseJoin{

	public ReducePhaseLeftJoin(RedisOP _lhs, RedisOP _rhs) {
		super(_lhs, _rhs);
	}

	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
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
				joinedRow.addAll(nullStrings);
				result.addRow(joinedRow);
			}
			
		}
		
		return result;
	}
	
	@Override
	public String toString(String indent) {
		return "ReducePhaseLeftJoin {\n" +
				indent + "  left : " + lhs.toString(indent + "  ") + "\n" +
				indent + "  right: " + rhs.toString(indent + "  ") + "\n" +
				indent + "}";
	}
	
}
