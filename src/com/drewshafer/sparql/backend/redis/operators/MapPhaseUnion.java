package com.drewshafer.sparql.backend.redis.operators;

import java.util.Set;
import java.util.Stack;

import com.drewshafer.sparql.backend.redis.QueryResult;
import com.drewshafer.sparql.backend.redis.RedisExpressionVisitor;

public class MapPhaseUnion implements RedisOP{

	RedisOP lhs;
	RedisOP rhs;
	
	public MapPhaseUnion(RedisOP _lhs, RedisOP _rhs){
		lhs = _lhs;
		rhs = _rhs;
	}
	
	@Override
	public String mapLuaScript() {
		String result = ""
				+ lhs.mapLuaScript()
				+ rhs.mapLuaScript()
				+ "\n"
				+ "log('MapPhaseUnion') \n"
				+ "\n"
				
				+ "local right = table.remove(mapResults) \n"
				+ "local left = table.remove(mapResults) \n"
				+ "local leftHeaders = left[1] \n"
				+ "local rightHeaders = right[1] \n"
				+ "local unionHeaders = {} \n"
				+ "for li, l in ipairs(leftHeaders) do \n"
				+ "  unionHeaders[li]=l \n"
				+ "end \n"
				+ "for ri, r in ipairs(rightHeaders) do \n"
				+ "  local found = -1 \n"
				+ "  for li, l in ipairs(leftHeaders) do \n"
				+ "    if r == l then \n"
				+ "      found = 1 \n"
				+ "      break \n"
				+ "    end \n"
				+ "  end \n"
				+ "  if found == -1 then \n"
				+ "    table.insert(unionHeaders, r) \n"
				+ "  end \n"
				+ "end \n"
				+ "local rightColMapping = {} \n"
				+ "local leftColMapping = {} \n"
				+ "for li, lh in ipairs(leftHeaders) do \n"
				+ "  for ui, uh in ipairs(unionHeaders) do \n"
				+ "    if lh==uh then \n"
				+ "      table.insert(leftColMapping, ui) \n"
				+ "      break \n"
				+ "    end \n"
				+ "  end \n"
				+ "end \n"
				+ "for ri, rh in ipairs(rightHeaders) do \n"
				+ "  for ui, uh in ipairs(unionHeaders) do \n"
				+ "    if rh==uh then \n"
				+ "      table.insert(rightColMapping, ui) \n"
				+ "      break \n"
				+ "    end \n"
				+ "  end \n"
				+ "end \n"
				
//				+ "log('leftHeaders is ' .. cjson.encode(leftHeaders)) \n"
//				+ "log('rightHeaders is ' .. cjson.encode(rightHeaders)) \n"
//				+ "log('unionHeaders is ' .. cjson.encode(unionHeaders)) \n"
//				+ "log('leftColMapping is ' .. cjson.encode(leftColMapping)) \n"
//				+ "log('rightColMapping is ' .. cjson.encode(rightColMapping)) \n"
//				+ "log('left input is ' .. cjson.encode(left)) \n"
//				+ "log('right input is ' .. cjson.encode(right)) \n"

				+ "local result = {} \n"
				+ "table.insert(result, unionHeaders) \n"
				+ "for i, row in ipairs(left) do \n"
				+ "  if not (i == 1) then \n"
				+ "    local newRow = {} \n"
				+ "    for j,x in ipairs(unionHeaders) do \n"
				+ "      table.insert(newRow, '@') \n"
				+ "    end \n"
				+ "    for li, lval in ipairs(row) do \n"
				+ "      newRow[leftColMapping[li]] = lval \n"
				+ "    end \n"
				+ "    table.insert(result, newRow) \n"
				+ "  end \n"
				+ "end \n"
				+ "for i, row in ipairs(right) do \n"
				+ "  if not(i == 1) then \n"
				+ "    local newRow = {} \n"
				+ "    for j,x in ipairs(unionHeaders) do \n"
				+ "      table.insert(newRow, '@') \n"
				+ "    end \n"
				+ "    for ri, rval in ipairs(row) do \n"
				+ "      newRow[rightColMapping[ri]] = rval \n"
				+ "    end \n"
				+ "    table.insert(result, newRow) \n"
				+ "  end \n"
				+ "end \n"			
//				+ "log('\\n##########union result##########: \\n' .. cjson.encode(result)) \n"
				
				+ "table.insert(mapResults, result) \n"
				+ "log('################################') \n"
				+ "log('MapPhaseUnion inserted result at index ' .. (#mapResults - 1)) \n"
//				+ "log('  headers: ' .. cjson.encode(thisMapResult[1])) \n"
//				+ "log('  ' .. (#thisMapResult - 1) .. ' rows') \n"
				+ "";
		return result;
	}

	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		return patternStack.pop();
	}

	@Override
	public Boolean completeAfterMapPhase() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public String toString(String indent) {
		return indent + "MapPhaseUnion {\n" +
			indent + "  left : " + lhs.toString(indent + "  ") + "\n" +
			indent + "  right: " + rhs.toString(indent + "  ") + "\n" +
			indent + "}";
	}

	@Override
	public Boolean tryAddFilter(RedisExpressionVisitor rev) {
		Boolean result = lhs.tryAddFilter(rev);
		result |= rhs.tryAddFilter(rev);
		return result;
	}

	@Override
	public RedisOP tryConvertToJoin(RedisOP op, Boolean left) {
		RedisOP attempt = lhs.tryConvertToJoin(op, left);
		if(attempt != null){
			this.lhs = attempt;
			return this;
		}
		attempt = rhs.tryConvertToJoin(op, left);
		if(attempt != null){
			this.rhs = attempt;
			return this;
		}
		return null;
	}

	@Override
	public Set<String> getJoinVariables() {
		Set<String> result = lhs.getJoinVariables();
		result.addAll(rhs.getJoinVariables());
		return result;
	}

}
