package translate.redis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import com.hp.hpl.jena.graph.Node;
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
	
	protected String joinSignature(List<Node> values, List<Integer> joinCols){
		StringBuilder result = new StringBuilder();
		for(Integer i:joinCols){
			result.append(values.get(i).toString());
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
	
	protected QueryResult _reduce(Stack<QueryResult> patternStack, Boolean leftJoin) {
	
		QueryResult right = rhs.reduce(patternStack);
		QueryResult left = lhs.reduce(patternStack);

		computeJoinCols(left, right);
		
		Map<String, List<List<Node>>> rhsJoinHash = new HashMap<String, List<List<Node>>>();
		for(List<Node> rightRow:right.rows){
			List<Node> rightKeepRow = new ArrayList<Node>();
			for(Integer r:rightKeepColIdxs){
				rightKeepRow.add(rightRow.get(r));
			}
			String joinSig = joinSignature(rightRow, joinColsRight);
			if(!rhsJoinHash.containsKey(joinSig)){
				List<List<Node>> matches = new ArrayList<List<Node>>();
				matches.add(rightKeepRow);
				rhsJoinHash.put(joinSig, matches);
			} else {
				rhsJoinHash.get(joinSig).add(rightKeepRow);
			}
		}
		
		QueryResult result = new QueryResult(joinedColNames);
		List<Node> nullNodes = new ArrayList<Node>();
		for(Integer r:rightKeepColIdxs){
			nullNodes.add(Node.createLiteral(""));
		}
		for(List<Node> leftRow: left.rows){
			List<Node> joinedRow = new ArrayList<Node>();
			joinedRow.addAll(leftRow);
			
			String joinSig = joinSignature(leftRow, joinColsLeft);
			List<List<Node>> rhsMatches = rhsJoinHash.get(joinSig);
			if(rhsMatches != null){
				for(List<Node> rightRow : rhsMatches)
				{
					List<Node> joinedRowN = new ArrayList<Node>();
					joinedRowN.addAll(joinedRow);
					joinedRowN.addAll(rightRow);
					result.addRow(joinedRowN);
				}
			} else if (leftJoin) {
				joinedRow.addAll(nullNodes);
				result.addRow(joinedRow);
			}
			
		}
		
		return result;
	}
	
	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		return this._reduce(patternStack, false);
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
