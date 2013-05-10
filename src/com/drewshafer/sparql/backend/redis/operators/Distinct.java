/**
 * Copyright (C) 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
//				+ "log('Distinct') \n"
				+ "for graphIdx, pattern in ipairs(mapResults) do \n"
//				+ "  log('before: mapResults[' .. graphIdx .. '] : ' .. (#pattern - 1) .. ' rows') \n"
				+ "  local newPattern = {}"
				+ "  local seen = {} \n"
				+ "  for i, row in ipairs(pattern) do \n"
				+ "    local rowJson = cjson.encode(row) \n"
				+ "    if not seen[rowJson] then \n"
				+ "      table.insert(newPattern, row) \n"
				+ "      seen[rowJson] = true \n"
				+ "    end \n"
				+ "  end \n"
//				+ "  log('before: mapResults[' .. graphIdx .. '] : ' .. (#newPattern - 1) .. ' rows') \n"
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
