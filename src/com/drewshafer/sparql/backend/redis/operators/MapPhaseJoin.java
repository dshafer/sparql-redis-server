package com.drewshafer.sparql.backend.redis.operators;

import java.util.Stack;

import com.drewshafer.sparql.backend.redis.QueryResult;

public class MapPhaseJoin extends RedisJoinOP {
	public MapPhaseJoin(RedisOP _lhs, RedisOP _rhs){
		super(_lhs, _rhs);
		
	}

	@Override
	public String mapLuaScript() {
		String result = " \n"
				+ lhs.mapLuaScript()
				+ rhs.mapLuaScript()
				+ "\n"
				+ "log('MapPhaseJoin') \n"
//				+ "local function hashNaturalJoin(left, right, joinCols) \n"
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
//				+ "      rval = joinTable[joinSig] \n"
//				+ "      for ri,rKeepIndex in ipairs(rightKeepCols) do \n"
//				+ "        table.insert(outputRow, rval[rKeepIndex]) \n"
//				+ "      end \n"
//				+ "    end \n"
//				+ "    table.insert(resultTable, outputRow) \n"
//				+ "  end \n"
//				+ "  return resultTable \n"
//				+ "end \n"
//				+ " \n"
//				+ "local function naturalJoin(left, right) \n"
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
//				+ "  return hashNaturalJoin(left,right,joinCols) \n"
//				+ "end \n"
				+ "\n"
				+ "local right = table.remove(mapResults) \n"
				+ "local left = table.remove(mapResults) \n"
				+ "table.insert(mapResults, naturalJoin(left, right)) \n"
				+ "log('################################') \n"
				+ "log('MapPhaseJoin inserted result at index ' .. (#mapResults - 1)) \n"
				+ "log('  headers: ' .. cjson.encode(thisMapResult[1])) \n"
				+ "log('  ' .. (#thisMapResult - 1) .. ' rows') \n"
				+ "log('') \n"
				+ "";
		
		return result;
	}

	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		return patternStack.pop();
	}


	@Override
	public String toString(String indent) {
		return indent + "MapPhaseJoin\n" +
				lhs.toString(indent + "  ") +
				rhs.toString(indent + "  ");
	}

	@Override
	public Boolean completeAfterMapPhase() {
		return true;
	}

}
