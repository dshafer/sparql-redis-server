package com.drewshafer.sparql.backend.redis.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import com.drewshafer.sparql.backend.redis.QueryResult;
import com.hp.hpl.jena.sparql.expr.Expr;

public abstract class RedisFilterOP implements RedisOP{
	
	List<Expr> expressions;
	RedisOP parent;
	public RedisFilterOP(RedisOP _parent){
		expressions = new ArrayList<Expr>();
		this.parent = _parent;
	}
	
	@Override
	public String mapLuaScript() {
		return parent.mapLuaScript();

	}
	
	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		return parent.reduce(patternStack);
	}

	public void addFilter(Expr e){
		expressions.add(e);
	}

	@Override
	public Set<String> getJoinVariables() {
		return parent.getJoinVariables();
	}
	
}
