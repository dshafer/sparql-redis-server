package translate.redis;

import java.util.ArrayList;
import java.util.Stack;

import redis.clients.jedis.Jedis;
import translate.redis.MapPhaseFilter.LuaFilter;

import com.hp.hpl.jena.sparql.expr.Expr;

import main.ShardedRedisTripleStore;

public class MapPhaseLeftJoin implements RedisOP {
	String leftJoinScript;
	public MapPhaseLeftJoin(){
		leftJoinScript = ""
				+ " \n"
				+ "local graphPatternKey = KEYS[1] \n"
				+ "local logKey = KEYS[2] \n"
				+ "redis.call('rpush', logKey, 'BGP') \n "
				+ "\n"
				+ "local function hashLeftJoin(left, right, joinCols) \n"
				+ "  local logKey = KEYS[3] \n"
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
				+ "  -- compute a table with the hash of the join keys from the right table \n"
				+ "  for ri, rval in ipairs(right) do \n"
				+ "    joinSig = '' \n"
				+ "    for i, joinCol in ipairs(joinCols) do \n"
				+ "      joinSig = joinSig .. rval[joinCol[3]] \n"
				+ "    end \n"
				+ "    joinTable[joinSig] = rval \n"
				+ "  end \n"
				+ "  for li, lval in ipairs(left) do \n"
				+ "    joinSig = '' \n"
				+ "    for i, joinCol in ipairs(joinCols) do \n"
				+ "      joinSig = joinSig .. lval[joinCol[2]] \n"
				+ "    end \n"
				+ "    local outputRow = lval \n"
				+ "    if joinTable[joinSig] then \n"
				+ "      -- has a counterpart in right.  Append the values \n"
				+ "      rval = joinTable[joinSig] \n"
				+ "      for ri,rKeepIndex in ipairs(rightKeepCols) do \n"
				+ "        table.insert(outputRow, rval[rKeepIndex]) \n"
				+ "      end \n"
				+ "    end \n"
				+ "    table.insert(resultTable, outputRow) \n"
				+ "    "
				+ "  end \n"
				+ "  return resultTable \n"
				+ "end \n"
				+ " \n"
				+ "local function naturalLeftJoin(left, right) \n"
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
				+ "  return hashNaturalJoin(left,right,joinCols) \n"
				+ "end \n"
				+ "\n"
				+ "local right = cjson.decode(redis.call('rpop', graphPatternKey)) \n"
				+ "local left = cjson.decode(redis.call('rpop', graphPatternKey)) \n"
				+ "local result = naturalLeftJoin(left, right) \n"
				+ "local resultJson = cjson.encode(result) \n"
				+ "redis.call('rpush', logKey, 'Join Result: ' .. resultJson) \n"
				+ "redis.call('rpush', graphPatternKey, resultJson) \n"
				+ "";
	}

	@Override
	public String mapLuaScript() {
		throw new UnsupportedOperationException();
	}

	@Override
	public QueryResult reduce(Stack<QueryResult> input) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Boolean completeAfterMapPhase() {
		return true;
	}

}
