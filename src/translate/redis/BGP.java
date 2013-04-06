package translate.redis;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import main.ShardedRedisTripleStore;
import main.DataTypes.GraphResult;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

import redis.clients.jedis.Jedis;
import translate.redis.RedisOP;
import org.json.*;

public class BGP implements RedisOP {
	StringBuilder luaTripleStringBuilder;
	String luaTripleDelimiter;
	
	public BGP() {
		luaTripleStringBuilder = new StringBuilder();
		luaTripleDelimiter = "";
	}
	
	public void cleanup(){
	
	}
	
	public void addTriple(String s, String p, String o){
		String luaTriple = "{'" + s + "'," + "'" + p + "'," + "'" + o + "'}";
		luaTripleStringBuilder.append(luaTripleDelimiter + luaTriple);
		luaTripleDelimiter = ",";
	}

	@Override
	public String mapLuaScript() {
		
		String result = ""
				+ "log('map -> BGP') \n"
				+ "local tripleList = { " + luaTripleStringBuilder.toString() + " } \n"
				+ "\n"
				+ "local function hashJoin(left, right, joinCols) \n"
				+ "  local joinTable = {} \n"
				+ "  local resultTable = {} \n"
				+ "  local joinSig = '' \n"
				+ "  local rightJoinCols = {} \n"
				+ "  for i, joinCol in ipairs(joinCols) do \n"
				+ "    rightJoinCols[joinCol[3]] = true \n"
				+ "  end \n"
				+ "  local rightKeepCols = {} \n"
				+ "  for ri,rColName in ipairs(right[1]) do \n"
				+ "    if not rightJoinCols[ri] then \n"
				+ "      table.insert(rightKeepCols, ri) \n"
				+ "    end \n"
				+ "  end \n"
				+ "  -- compute a table with the hash of the join keys from the left table \n"
				+ "  for l, lval in ipairs(left) do \n"
				+ "    joinSig = '' \n"
				+ "    for i, joinCol in ipairs(joinCols) do \n"
				+ "      joinSig = joinSig .. lval[joinCol[2]] \n"
				+ "    end \n"
				+ "    joinTable[joinSig] = lval \n"
				+ "  end \n"
				+ "  for r, rval in ipairs(right) do \n"
				+ "    joinSig = '' \n"
				+ "    for i, joinCol in ipairs(joinCols) do \n"
				+ "      joinSig = joinSig .. rval[joinCol[3]] \n"
				+ "    end \n"
				+ "    if joinTable[joinSig] then \n"
				+ "      -- found a good row.  Need to join it together and append the result \n"
				+ "      local outputRow = {} \n"
				+ "      for li,lItem in ipairs(joinTable[joinSig]) do \n"
				+ "        table.insert(outputRow, lItem) \n"
				+ "      end \n"
				+ "      for ri,rKeepIndex in ipairs(rightKeepCols) do \n"
				+ "        table.insert(outputRow, rval[rKeepIndex]) \n"
				+ "      end \n"
				+ "      table.insert(resultTable, outputRow) \n"
				+ "    end \n"
				+ "  end \n"
				+ "  return resultTable \n"
				+ "end \n"
				+ "local function naturalJoin(left, right) \n"
				+ "  -- determine join columns\n"
				+ "  local joinCols = {} \n"
				+ "  for l,lColName in ipairs(left[1]) do \n"
				+ "    for r,rColName in ipairs(right[1]) do \n"
				+ "      if lColName == rColName then \n"
				+ "        table.insert(joinCols, {lColName, l, r}) \n"
				+ "        break \n"
				+ "      end \n"
				+ "    end \n"
				+ "  end \n"
				+ "  return hashJoin(left,right,joinCols) \n"
				+ "end \n"
				+ "local function split(pString, pPattern) \n"
				+ "  local Table = {}  -- NOTE: use {n = 0} in Lua-5.0 \n"
				+ "  local fpat = '(.-)' .. pPattern \n"
				+ "  local last_end = 1 \n"
				+ "  local s, e, cap = pString:find(fpat, 1) \n"
				+ "  while s do \n"
				+ "    if s ~= 1 or cap ~= '' then \n"
				+ "      table.insert(Table,cap) \n"
				+ "    end \n"
				+ "    last_end = e+1 \n"
				+ "    s, e, cap = pString:find(fpat, last_end) \n"
				+ "  end \n"
				+ "  if last_end <= #pString then \n"
				+ "    cap = pString:sub(last_end) \n"
				+ "    table.insert(Table, cap) \n"
				+ "  end \n"
				+ "  return Table \n"
				+ "end \n"
				+ "\n"
				+ "local function getLiteralFromAlias(alias) \n"
				+ "  return '!' .. redis.call('hget', 'literalLookup', alias) \n"
				+ "end \n"
				+ "\n"
				+ "local searchFuncs = {} \n"
				+ "local alteredVars = {} \n"
				+ "local tripleTables = {} \n"
				+ "local thisMapResult = {} \n"
				+ "local searchCtrl = {'S', 'P', 'O'} \n"
				+ "for ti, triplePattern in ipairs(tripleList) do \n"
				+ "  -- triplePattern is an array of [s, p, o] \n"
				+ "  local searchType = '' \n"
				+ "  local searchKey = '' \n"
				+ "  local outputVarList = {} \n"
				+ "  local patternResult = {} \n"
				+ "  for i,pattern in ipairs(triplePattern) do \n"
				+ "    if string.find(pattern, '^%?') then \n"
				+ "      table.insert(outputVarList, pattern) \n"
				+ "    else \n"
				+ "      searchType = searchType .. searchCtrl[i] \n"
				+ "      searchKey = searchKey .. ':' .. pattern \n"
				+ "    end \n"
				+ "  end \n"
				+ "  table.insert(patternResult, outputVarList) \n"
				+ "  searchKey = searchType .. searchKey \n"
				+ "  log('searchKey is ' .. searchKey) \n"
				+ "  local patternMembers = redis.call('smembers', searchKey) \n"
				+ "  for i,pattern in ipairs(patternMembers) do \n"
				+ "    table.insert(patternResult, split(pattern, ':')) \n"
				+ "  end \n"
				+ "  table.insert(tripleTables, patternResult) \n"
				+ "end \n"
				+ " \n"
				+ "-- do the joins over the various BGP triples \n"
				+ "thisMapResult = table.remove(tripleTables, 1) \n"
				+ "local joinCount = 0 \n"
				+ "local extra = table.remove(tripleTables, 1) \n"
				+ "while extra do \n"
				+ "  thisMapResult = naturalJoin(thisMapResult, extra) \n"
				+ "  joinCount = joinCount + 1 \n"
				+ "  extra = table.remove(tripleTables, 1) \n"
				+ "end \n"
				+ " \n"
				+ "log('After Joins: thisMapResult is ' .. cjson.encode(thisMapResult)) \n"
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
				+ "table.insert(mapResults, thisMapResult) \n"
				+ "log('Final (translated) BGP result is ' .. cjson.encode(thisMapResult)) \n"
				+ "\n"
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

}
