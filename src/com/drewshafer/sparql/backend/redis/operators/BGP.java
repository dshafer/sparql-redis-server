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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import com.drewshafer.sparql.backend.redis.QueryResult;
import com.drewshafer.sparql.backend.redis.RedisExpressionVisitor;
import com.hp.hpl.jena.sparql.core.Var;

public class BGP implements RedisOP {
	Set<String> variables;
	List<RedisExpressionVisitor> filterExpressions;
	Set<String> subjectVariables;
	Set<String> objectVariables;
	List<List<String>> triples;
	int id;
	static int idTracker = 0;
	
	
	public BGP() {
		id = idTracker++;
		triples = new ArrayList<List<String>>();
		subjectVariables = new HashSet<String>();
		objectVariables = new HashSet<String>();
		variables = new HashSet<String>();
		filterExpressions = new ArrayList<RedisExpressionVisitor>();
	}
	
	public void cleanup(){
	
	}
	
	public void addTriple(String s, String p, String o){
		List<String> triple = new ArrayList<String>(3);
		triple.add(s);
		triple.add(p);
		triple.add(o);
		triples.add(triple);
		if(!s.startsWith("!")){
			s = s.startsWith("?") ? s.substring(1) : s;
			variables.add(s);
			subjectVariables.add(s);
		}		
		if(!p.startsWith("!")){
			variables.add(p.substring(1));
		}		
		if(!o.startsWith("!")){
			variables.add(o.substring(1));
			objectVariables.add(o.substring(1));
		}
	}
	
	private String luaTripleString(){
		StringBuilder sb = new StringBuilder();
		String delimiter = "";
		for(List<String> triple: triples){
			String s = triple.get(0);
			String p = triple.get(1);
			String o = triple.get(2);
			String luaTriple = "{'" + s + "'," + "'" + p + "'," + "'" + o + "'}";
			sb.append(delimiter + luaTriple);
			delimiter = ",";
		}
		return sb.toString();
	}
	
	
	private void appendTripleTables(String joinVar, StringBuilder sb){
		
		for(List<String> triple : triples){
			String s = triple.get(0);
			String p = triple.get(1);
			String o = triple.get(2);
			String sH;
			String pH;
			String oH;
			List<String> searchKeys = new ArrayList<String>();
			String headerString;
			
			if(s.startsWith("?")){
				sH = s;
			} else {
				sH = "?META_JOIN_" + s;
				searchKeys.add("S:" + s);
			}
			if(p.startsWith("?")){
				pH = p;
			} else {
				pH = "?META_JOIN_" + p;
				searchKeys.add("P:" + p);
			}
			if(o.startsWith("?")){
				oH = o;
			} else {
				oH = "?META_JOIN_" + o;
				searchKeys.add("O:" + o);
			}		
			
			String redisCmd;
			if(searchKeys.size() == 1){
				redisCmd = "redis.call('smembers', '" + searchKeys.get(0) + "') ";
			} else {
				redisCmd = "redis.call('sinter', '" + searchKeys.get(0) + "', '" + searchKeys.get(1) + "') ";
			}
		
			
//			int joinIdx = -1;
//			if(sH.equals(joinVar)){
//				joinIdx = 1;
//				headerString = "{'" + sH + "','" + pH + "','" + oH + "'}";
//			} else if(pH.equals(joinVar)){
//				joinIdx = 2;
//				headerString = "{'" + pH + "','" + sH + "','" + oH + "'}";
//			} else if(oH.equals(joinVar)){
//				joinIdx = 3;
//				headerString = "{'" + oH + "','" + sH + "','" + pH + "'}";
//			} else {
//				throw new UnsupportedOperationException();
//			}
			
			
			sb.append("table.insert(tripleTables, decodeTriples({'" + sH + "','" + pH + "','" + oH + "'}, " + redisCmd + ") ) \n");
		}
	}

	@Override
	public String mapLuaScript() {
		Set<String> joinVars = getJoinVariables();
		if(joinVars.size() != 1){
			throw new UnsupportedOperationException();
		}
		String bareJoinVar = joinVars.iterator().next();
		String joinVar;
		if(bareJoinVar.indexOf(':') > 0){
			joinVar = "?META_JOIN_" + bareJoinVar;
		} else {
			joinVar = "?" + bareJoinVar;
		}
		
		
		
		StringBuilder result = new StringBuilder( ""
//				+ "log('map -> BGP_" + id + "') \n"
				+ "local tripleTables = {} \n");
		
		appendTripleTables(joinVar, result);
		
		result.append(""
				+ "-- sort the triple tables largest to smallest \n"
				+ "table.sort(tripleTables, function(a,b) return #a > #b end) \n"
				+ "local thisMapResult = table.remove(tripleTables) \n"
				+ "while #tripleTables > 0 do \n"
				+ "  thisMapResult = naturalJoin(thisMapResult, table.remove(tripleTables)) \n"
				+ "end \n"				
				+ "-- translate the literals \n"
				+ "for i,row in ipairs(thisMapResult) do \n"
				+ "  for j,val in ipairs(row) do \n"
				+ "    if string.find(val, '^#') then \n"
				+ "      row[j] = getLiteralFromAlias(val) \n"
				+ "    end \n"
				+ "  end \n"
				+ "  thisMapResult[i] = row \n"
				+ "end \n"
				+ " \n"
//				+ "log('  ' .. (#thisMapResult - 1) .. ' rows') \n"
				+ "\n"
				+ "");
		
		if(filterExpressions.size() > 0){
			String delim = "";
			StringBuilder luaFunc = new StringBuilder();
			for(RedisExpressionVisitor rev: filterExpressions){
				luaFunc.append(delim +  rev.getLuaFunctionExpression());
				delim = " and ";
			}
			result.append(""
//					+ "log('BGP_" + id + " -> Filter') \n"
					+ "local filterFunc = function(vars) return(" + luaFunc.toString() + ") end \n"
					+ "\n"
					+ "local newResult = {} \n"
					+ "table.insert(newResult, thisMapResult[1]) \n"
					+ "local varNames = thisMapResult[1] \n"
					+ "for i,row in ipairs(thisMapResult) do \n"
					+ "  if not (i == 1) then \n"
					+ "    local vars = {} \n"
					+ "    for vi,varName in ipairs(varNames) do \n"
					+ "      vars[varName] = row[vi] \n"
					+ "    end \n"
					+ "    local success, passedFilter = pcall(filterFunc, vars) \n"
					+ "    if not success then \n"
//					+ "      log('!!!! bad filter eval.  vars is ' .. cjson.encode(vars)) \n"
//					+ "      log('error message: ' .. passedFilter) \n"
					+ "    elseif passedFilter then \n"
					+ "      table.insert(newResult, row) \n"
					+ "    end \n"
					+ "  end \n"
					+ "end \n"
					+ "thisMapResult = newResult \n"
					+ "");
		}
		
		result.append("" 
				+ "table.insert(mapResults, thisMapResult) \n"
//				+ "log('################################') \n"
//				+ "log('BGP_" + id + " inserted result at index ' .. (#mapResults - 1)) \n"
//				+ "log('  headers: ' .. cjson.encode(thisMapResult[1])) \n"
//				+ "log('  ' .. (#thisMapResult - 1) .. ' rows') \n"
//				+ "log('Final (translated) BGP result is ' .. cjson.encode(thisMapResult)) \n"
//				+ "log('') \n"
				);
		
		return result.toString();

		
	}

	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		QueryResult result = patternStack.pop();
		Set<String> resultVars = new HashSet<String>();
		for(String colName : result.columnNames){
//			if(!colName.startsWith("?META")){
				resultVars.add(colName.substring(1));
//			}
		}
		if(!resultVars.equals(variables)){
			int x = 0;
			int y = x;
		}
		return result;
	}

	@Override
	public Boolean completeAfterMapPhase() {
		return true;
	}

	@Override
	public String toString(String indent) {
		StringBuilder sb = new StringBuilder();
		sb.append("BGP_" + id + " {\n");
		//String extraIndent = "";
		String delimiter = "";
		if(filterExpressions.size() > 0){
			sb.append(indent + "  filter: {");
			//sb.append(indent + "  expr: (");
			for(RedisExpressionVisitor e: filterExpressions){
				sb.append(delimiter + e.getLuaFunctionExpression());
				delimiter = " and ";
			}
			//extraIndent = "  ";
			sb.append("},\n");
		} 
		sb.append(indent + "  pattern : { ");
		delimiter = "";
		for(List<String> triple : triples){
			String jsonLine = "{'" + triple.get(0) + "','" + triple.get(1) + "','" + triple.get(2) + "'}";
			sb.append(delimiter + jsonLine);
			delimiter = ",";
		}
		sb.append(" }\n");
		
		sb.append(indent + "}");
			
		return sb.toString();
	}

	@Override
	public Boolean tryAddFilter(RedisExpressionVisitor rev) {
		Set<String> varsMentioned = new HashSet<String>();
		for(Var v:rev.getVarsMentioned()){
			varsMentioned.add(v.getVarName());
		}
		if(variables.containsAll(varsMentioned)){
			Map<String, String> equalities = rev.getEqualities();
			for(String varName : equalities.keySet()){
				String varVal = equalities.get(varName);
				for(List<String> triple : triples){
					for(int i = 0; i < 3; i++){
						if(triple.get(i).equals(varName)){
							triple.set(i, varVal);
						}
					}
				}
			}
			if(rev.hasNonEqualities()){
				filterExpressions.add(rev);
			}
			return true;
		} else {
			return false;
		}
			
	}
	
	@Override
	public RedisOP tryConvertToJoin(RedisOP op, Boolean left) {
		if (this.getJoinVariables().containsAll(op.getJoinVariables())){
			return left ? new MapPhaseLeftJoin(this, op) : new MapPhaseJoin(this, op);
		}
		return null;
	}

	@Override
	public Set<String> getJoinVariables() {
		Set<String> result = new HashSet<String>(subjectVariables);
		if(result.size() == 1){
			return result;
		} else {
			result.retainAll(objectVariables);
			return result;
		}
	}

}
