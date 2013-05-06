package com.drewshafer.sparql.backend.redis.operators;

import java.util.Stack;

import com.drewshafer.sparql.backend.redis.QueryResult;

public class ReducePhaseLeftJoin extends ReducePhaseJoin{

	public ReducePhaseLeftJoin(RedisOP _lhs, RedisOP _rhs) {
		super(_lhs, _rhs);
	}

	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		return super._reduce(patternStack, true);
	}
	
	@Override
	public String toString(String indent) {
		return "ReducePhaseLeftJoin {\n" +
				indent + "  left : " + lhs.toString(indent + "  ") + "\n" +
				indent + "  right: " + rhs.toString(indent + "  ") + "\n" +
				indent + "}";
	}
	
}
