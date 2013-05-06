package com.drewshafer.sparql.backend.redis.operators;

import java.util.Set;
import java.util.Stack;

import com.drewshafer.sparql.backend.redis.QueryResult;
import com.drewshafer.sparql.backend.redis.RedisExpressionVisitor;

public class ReducePhaseSlice implements RedisOP {

	RedisOP parent;
	long start;
	long length;
	public ReducePhaseSlice(RedisOP _parent, long _start, long _length){
		this.start = 0;
		if(_start != Long.MIN_VALUE){
			this.start = _start;
		}
		this.length = Long.MAX_VALUE;
		if(_length != Long.MIN_VALUE){
			this.length = _length;
		}
		this.parent = _parent;
	}
	
	@Override
	public String mapLuaScript() {
		return parent.mapLuaScript();
	}

	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		QueryResult pR = parent.reduce(patternStack);
		QueryResult result = new QueryResult(pR.columnNames);
		long prLim = this.start + pR.rows.size();
		for(long x = 0; (x < this.start + this.length) && (x < prLim) && (x < pR.rows.size()); x++){
			result.addRow(pR.rows.get((int)x));
		}
		return result;
	}

	@Override
	public Boolean completeAfterMapPhase() {
		return false;
	}

	@Override
	public String toString(String indent) {
		StringBuilder sb = new StringBuilder();
		sb.append("ReducePhaseSlice {\n");
		sb.append(indent + "  start : " + this.start + ",\n");
		sb.append(indent + "  length: " + this.length + ",\n");
		sb.append(indent + "  parent: " + parent.toString(indent + "  "));
		sb.append(indent + "}\n");
		return sb.toString();
	}

	@Override
	public Boolean tryAddFilter(RedisExpressionVisitor rev) {
		return parent.tryAddFilter(rev);
	}

	@Override
	public RedisOP tryConvertToJoin(RedisOP op, Boolean left) {
		RedisOP attempt = parent.tryConvertToJoin(op, left);
		if(attempt != null){
			this.parent = attempt;
			return this;
		}
		return null;
	}

	@Override
	public Set<String> getJoinVariables() {
		return parent.getJoinVariables();
	}

}
