package com.drewshafer.sparql.backend.redis.operators;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;


import com.drewshafer.sparql.backend.redis.QueryResult;
import com.drewshafer.sparql.backend.redis.RedisExpressionVisitor;
import com.hp.hpl.jena.graph.Node;

public class Distinct implements RedisOP {

	String distinctScript;
	RedisOP parent;
	
	public Distinct(RedisOP _parent){
		this.parent = _parent;
	}


	
	@Override
	public String mapLuaScript() {
		return ""
				+ parent.mapLuaScript()
				+ "  \n"
				+ "log('Distinct') \n"
				+ "for graphIdx, pattern in ipairs(mapResults) do \n"
				+ "  log('before: mapResults[' .. graphIdx .. '] : ' .. (#pattern - 1) .. ' rows') \n"
				+ "  local newPattern = {}"
				+ "  local seen = {} \n"
				+ "  for i, row in ipairs(pattern) do \n"
				+ "    local rowJson = cjson.encode(row) \n"
				+ "    if not seen[rowJson] then \n"
				+ "      table.insert(newPattern, row) \n"
				+ "      seen[rowJson] = true \n"
				+ "    end \n"
				+ "  end \n"
				+ "  log('before: mapResults[' .. graphIdx .. '] : ' .. (#newPattern - 1) .. ' rows') \n"
				+ "  mapResults[graphIdx] = newPattern \n"
				+ "end \n"
				+ "";
	}



	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		QueryResult origResult = parent.reduce(patternStack);
		QueryResult result = new QueryResult(origResult.columnNames);
		
		Set<String> seenRows = new HashSet<String>();
		for(List<Node> row : origResult.rows){
			StringBuilder sb = new StringBuilder();
			for(Node n:row){
				sb.append(n.toString());
			}
			String sig = sb.toString();
			if(!seenRows.contains(sig)){
				result.addRow(row);
				seenRows.add(sig);
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
		return "Distinct {\n" +
				indent + "  " + parent.toString(indent + "  ") + "\n" +
				indent + "}\n";
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
