package com.drewshafer.sparql.backend.redis.operators;

import java.util.ArrayList;
import java.util.Set;
import java.util.Stack;


import com.drewshafer.sparql.backend.redis.QueryResult;
import com.drewshafer.sparql.backend.redis.RedisExpressionVisitor;
import com.hp.hpl.jena.sparql.expr.Expr;


public class MapPhaseOrder implements RedisOP{
	
	RedisExpressionVisitor rev;
	RedisOP parent;
	public MapPhaseOrder(RedisOP _parent, RedisExpressionVisitor _rev){
		parent = _parent;
		rev = _rev;
	}
	
	@Override
	public String mapLuaScript() {
		
		StringBuilder result = new StringBuilder(parent.mapLuaScript());
		
		result.append( 
			  "  \n"
//			+ "log('map -> Order') \n"
			+ "local rowScoreFunc = function(vars) return (" + rev.getLuaFunctionExpression() + ") end \n"
			+ "-- add the sort column \n"
			+ "\n"
			+ "for i,mapResult in ipairs(mapResults) do\n"
			+ "  local newResult = {} \n"
			+ "  local varNames = mapResult[1] \n"
			+ "  local sortKeyIdx = #varNames + 1 \n"
			+ "  for i,row in ipairs(mapResult) do \n"
			+ "    if not (i == 1) then \n"
			+ "      local vars = {} \n"
			+ "      for vi,varName in ipairs(varNames) do \n"
			+ "        vars[varName] = row[vi] \n"
			+ "      end \n"
			+ "      local score = {v= rowScoreFunc(vars)} \n"
			+ "      table.insert(row, score) \n"
			+ "      table.insert(newResult, row) \n"
			+ "    end \n"
			+ "  end \n"
			+ "   \n"
			+ "  local sortFn = function(a,b) \n"
			+ "    return a[sortKeyIdx]['v'] < b[sortKeyIdx]['v'] \n"
			+ "  end \n"
			+ "  table.sort(newResult, sortFn) \n"
			+ "  mapResults[i] = {} \n"
			+ "  table.insert(varNames, 'META_SORT_ASC') \n"
			+ "  table.insert(mapResults[i], varNames) \n"
			+ "  for ii, row in ipairs(newResult) do \n"
			+ "  	table.insert(mapResults[i], row) \n"
			+ "  end \n"
			+ "end \n"
			+ "");
		return result.toString();
		
	}

	@Override
	public Boolean completeAfterMapPhase() {
		return true;
	}

	@Override
	public String toString(String indent) {
		StringBuilder sb = new StringBuilder();
		sb.append("MapPhaseOrder{\n");
		sb.append(indent + "  luaFunc: " + rev.getLuaFunctionExpression() + ",\n");
		sb.append(indent + "  parent : ");
		sb.append(parent.toString(indent + "    ") + "\n");
		sb.append(indent + "}");
		return sb.toString();
	}

	@Override
	public RedisOP tryConvertToJoin(RedisOP op, Boolean left) {
		RedisOP attempt = parent.tryConvertToJoin(op, left);
		if(attempt != null){
			parent = attempt;
			return this;
		} else {
			return null;
		}
	}

	@Override
	public Boolean tryAddFilter(RedisExpressionVisitor rev) {
		return parent.tryAddFilter(rev);
	}

	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		// TODO
		return parent.reduce(patternStack);
	}

	@Override
	public Set<String> getJoinVariables() {
		return parent.getJoinVariables();
	}
	
}
