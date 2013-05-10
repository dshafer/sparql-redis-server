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

import java.util.Stack;

import com.drewshafer.sparql.backend.redis.QueryResult;

public class MapPhaseLeftJoin extends RedisJoinOP {
	public MapPhaseLeftJoin(RedisOP _lhs, RedisOP _rhs){
		super(_lhs, _rhs);
	}

	@Override
	public String mapLuaScript() {
		String result = ""
				+ lhs.mapLuaScript()
				+ rhs.mapLuaScript()
				+ "\n"
//				+ "log('MapPhaseLeftJoin') \n"
//				+ "\n"
//				+ "local function hashNaturalLeftJoin(left, right, joinCols) \n"
//				+ "  local joinTable = {} \n"
//				+ "  local resultTable = {} \n"
//				+ "  local joinSig = '' \n"
//				+ "  local rightJoinCols = {} \n"
//				+ "  for i, joinCol in ipairs(joinCols) do \n"
//				+ "    rightJoinCols[joinCol[3]] = true \n"
//				+ "  end \n"
//				+ "  local rightKeepCols = {} \n"
//				+ "  for ri,rColName in ipairs(right[1]) do \n"
//				+ "    if not rightJoinCols[ri] then \n"
//				+ "      table.insert(rightKeepCols, ri) \n"
//				+ "    end \n"
//				+ "  end \n"
//				+ "  -- compute a table with the hash of the join keys from the right table \n"
//				+ "  for ri, rval in ipairs(right) do \n"
//				+ "    joinSig = '' \n"
//				+ "    for i, joinCol in ipairs(joinCols) do \n"
//				+ "      joinSig = joinSig .. rval[joinCol[3]] \n"
//				+ "    end \n"
//				+ "    joinTable[joinSig] = rval \n"
//				+ "  end \n"
//				+ "  for li, lval in ipairs(left) do \n"
//				+ "    joinSig = '' \n"
//				+ "    for i, joinCol in ipairs(joinCols) do \n"
//				+ "      joinSig = joinSig .. lval[joinCol[2]] \n"
//				+ "    end \n"
//				+ "    local outputRow = lval \n"
//				+ "    if joinTable[joinSig] then \n"
//				+ "      -- has a counterpart in right.  Append the values \n"
//				+ "      local rval = joinTable[joinSig] \n"
//				+ "      for ri,rKeepIndex in ipairs(rightKeepCols) do \n"
//				+ "        table.insert(outputRow, rval[rKeepIndex]) \n"
//				+ "      end \n"
//				+ "    else \n"
//				+ "      -- no counterpart in right.  Append 'null' values encoded as'@' \n"
//				+ "      for ri,rKeepIndex in ipairs(rightKeepCols) do \n"
//				+ "        table.insert(outputRow, '@') \n"
//				+ "      end \n"
//				+ "    end \n"
//				+ "    table.insert(resultTable, outputRow) \n"
//				+ "  end \n"
//				+ "  return resultTable \n"
//				+ "end \n"
//				+ " \n"
//				+ "local function naturalLeftJoin(left, right) \n"
//				+ "  -- determine join columns\n"
//				+ "  local joinCols = {} \n"
//				+ "  for l,lColName in ipairs(left[1]) do \n"
//				+ "    for r,rColName in ipairs(right[1]) do \n"
//				+ "      if lColName == rColName then \n"
//				+ "        table.insert(joinCols, {lColName, l, r}) \n"
//				+ "        break \n"
//				+ "      end \n"
//				+ "    end \n"
//				+ "  end \n"
//				+ "  return hashNaturalLeftJoin(left,right,joinCols) \n"
//				+ "end \n"
				+ "\n"
				+ "local right = table.remove(mapResults) \n"
				+ "local left = table.remove(mapResults) \n"
				+ "table.insert(mapResults, naturalLeftJoin(left, right)) \n"
//				+ "log('################################') \n"
//				+ "log('MapPhaseLeftJoin inserted result at index ' .. (#mapResults - 1)) \n"
//				+ "log('  headers: ' .. cjson.encode(thisMapResult[1])) \n"
//				+ "log('  ' .. (#thisMapResult - 1) .. ' rows') \n"
//				+ "log('') \n"
				+ "";
		return result;
	}

	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		return patternStack.pop();
	}

	@Override
	public Boolean completeAfterMapPhase() {
		return true;
	}

	@Override
	public String toString(String indent) {
		return "MapPhaseLeftJoin {\n" +
				indent + "  left : " + lhs.toString(indent + "  ") + "\n" +
				indent + "  right: " + rhs.toString(indent + "  ") + "\n" +
				indent + "}";
	}

}
