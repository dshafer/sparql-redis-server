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

import com.hp.hpl.jena.sparql.expr.Expr;
public abstract class MapPhaseFilter extends RedisFilterOP{
	

	public MapPhaseFilter(RedisOP _parent){
		super(_parent);
		expressions = new ArrayList<Expr>();
	}
	
	private String luaFunctionExpression(){
		StringBuilder sb = new StringBuilder();
		sb.append("return function(vars) return ");
		String delimeter = "";
		for(Expr e: expressions){
			//LuaFilter lf = new LuaFilter();
//			e.visit(lf);
//			sb.append(delimeter + lf.getLuaFunctionExpression());
//			delimeter = "&&";
		}
		sb.append(" end ");
		return sb.toString();
	}
	
	@Override
	public String mapLuaScript() {
		
		String result =  parent.mapLuaScript();
		String luaFunc = luaFunctionExpression();
		
		result += 
			  "  \n"
//			+ "log('map -> Filter') \n"
//			+ "local filterFuncBody = \"" + luaFunc + "\" \n"
//			+ "log('compiling: ' .. filterFuncBody) \n"
//			+ "local filterFunc = loadstring(filterFuncBody)() \n"
			+ "local filterFunc = function(vars) return(" + luaFunc + ") end \n"
			+ "\n"
			+ "for i,mapResult in ipairs(mapResults) do\n"
			+ "  local newResult = {} \n"
			+ "  table.insert(newResult, mapResult[1]) \n"
			+ "  local varNames = mapResult[1] \n"
			+ "  for i,row in ipairs(mapResult) do \n"
			+ "    if not (i == 1) then \n"
			+ "      local vars = {} \n"
			+ "      for vi,varName in ipairs(varNames) do \n"
			+ "        if string.find(row[vi], '^!') then \n"
			+ "          vars[varName] = string.sub(row[vi], 2, -1) \n"
			+ "        else \n"
			+ "          vars[varName] = row[vi] \n"
			+ "        end"
			+ "      end \n"
//			+ "      log('vars is ' .. cjson.encode(vars)) \n"
			+ "      if filterFunc(vars) then \n"
//			+ "        log('  passed filter') \n"
			+ "        table.insert(newResult, row) \n"
			+ "      end \n"
			+ "    end \n"
			+ "  end \n"
			+ "   \n"
			+ "  mapResults[i] = newResult \n"
			+ "end \n"
//			+ "log('MapPhaseFilter: After all joins, thisMapResult has ' .. (#thisMapResult - 1) .. ' rows') \n"
			
			+ "";
		return result;
		
	}

	@Override
	public Boolean completeAfterMapPhase() {
		return true;
	}

	@Override
	public String toString(String indent) {
		StringBuilder sb = new StringBuilder();
		sb.append(indent + "MapPhaseFilter{,\n");
		sb.append(indent + "  luaFunc: " + luaFunctionExpression() + ",\n");
		sb.append(indent + "  parent : \n");
		sb.append(parent.toString(indent + "    "));
		sb.append(indent + "},");
		return sb.toString();
	}

	@Override
	public RedisOP tryConvertToJoin(RedisOP op, Boolean left) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
