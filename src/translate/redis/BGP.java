package translate.redis;
import java.util.ArrayList;
import java.util.List;

import main.ShardedRedisTripleStore;
import main.DataTypes.GraphResult;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

import redis.clients.jedis.Jedis;
import translate.redis.RedisOP;
import org.json.*;

public class BGP implements RedisOP {
	List<Triple> triples;
	String bgpMatchScript;
	String tripleKey;
	String resultKey;
	List<String> projectedVariables;
	
	public BGP() {
		triples = new ArrayList<Triple>();
		
		// bgp match script takes three args
		// Key1: Redis key containing list of triple patterns to match
		// Key2: Redis key containing Graph Pattern Data Input.
		// Key3: Redis key to store Graph Pattern Data Output
		// Returns: 1 if the entire BGP was processed.
		//          0 if entire BGP could not be processed.  Caller should UNION results with other nodes and call bgpMatchScript again
		
		
		bgpMatchScript = ""
				+ "  \n"
				+ "local tripleListKey = KEYS[1] \n"
				+ "local graphPatternKey = KEYS[2] \n"
				+ "local logKey = KEYS[3] \n"
				+ "\n"
				+ "local function hashJoin(left, right, joinCols) \n"
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
				+ "      redis.call('rpush', logKey, 'match:' .. cjson.encode(outputRow))"
				+ "      table.insert(resultTable, outputRow) \n"
				+ "    end \n"
				+ "    "
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
				+ "\n"
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
				+ "local function getLiteralAlias(l) \n"
				+ "  if redis.call('hexists', 'literalAliases', l) == 1 then \n"
				+ "    return redis.call('hget', 'literalAliases', l) \n"
				+ "  end \n"
				+ "  local lAlias = '#' .. redis.call('hlen', 'literalAliases') \n"
				+ "  redis.call('hset', 'literalAliases', l, lAlias) \n"
				+ "  redis.call('hset', 'literalLookup', lAlias, l) \n"
				+ "  return lAlias \n"
				+ "end \n"
				+ "local function getLiteralFromAlias(alias) \n"
				+ "  return '!' .. redis.call('hget', 'literalLookup', alias) \n"
				+ "  -- return 'blah' -- alias \n"
				+ "end \n"
				+ "\n"
				+ "local searchFuncs = {} \n"
				+ "local alteredVars = {} \n"
				+ "local tripleTables = {} \n"
				+ "-- if there's an input graph pattern, it becomes the initial tripleResult \n"
				+ "local inputGraphPatternJson = redis.call('get', graphPatternKey) \n"
				+ "if inputGraphPatternJson then \n"
				+ "  table.insert(tripleTables, cjson.decode(inputGraphPatternJson)) \n"
				+ "end \n"
				+ "-- if redis.call('llen', graphPatternKey) > 0 then \n"
				+ "--   local inputTable = {} \n"
				+ "--   while redis.call('llen', graphPatternKey) > 0 do \n"
				+ "--     table.insert(inputTable, cjson.decode(redis.call('lpop', graphPatternKey))) \n"
				+ "--   end \n"
				+ "--   table.insert(tripleTables, inputTable) \n"
				+ "-- end \n"
				+ "local searchCtrl = {'S', 'P', 'O'} \n"
				+ "while redis.call('llen', tripleListKey) > 0 do \n"
				+ "  local triplePatternJson = redis.call('lpop', tripleListKey) \n"
				+ "  redis.call('rpush', logKey, 'Processing Triple pattern' .. triplePatternJson) \n"
				+ "  local triplePattern = cjson.decode(triplePatternJson) \n"
				+ "  -- triplePattern is an array of [s, p, o] \n"
				+ "  \n"
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
				+ "  redis.call('rpush', logKey, 'searchKey is ' .. searchKey) \n"
				+ "  local patternMembers = redis.call('smembers', searchKey) \n"
				+ "  for i,pattern in ipairs(patternMembers) do \n"
				+ "    table.insert(patternResult, split(pattern, ':')) \n"
				+ "  end \n"
				+ "  table.insert(tripleTables, patternResult) \n"
				+ "end \n"
				+ " \n"
				+ "-- do the joins over the various BGP triples \n"
				+ "local result = table.remove(tripleTables, 1) \n"
				+ "local joinCount = 0 \n"
				//+ "redis.call('rpush', logKey, joinCount .. ' Joins: result is ' .. cjson.encode(result)) \n"
				+ "local extra = table.remove(tripleTables, 1) \n"
				+ "while extra do \n"
				+ "  result = naturalJoin(result, extra) \n"
				+ "  joinCount = joinCount + 1 \n"
				//+ "  redis.call('rpush', logKey, joinCount .. ' Joins: result is ' .. cjson.encode(result)) \n"
				+ "  extra = table.remove(tripleTables, 1) \n"
				+ "end \n"
				+ " \n"
				+ "redis.call('rpush', logKey, joinCount .. ' After Joins: result is ' .. cjson.encode(result)) \n"
				+ "-- translate the literals \n"
				+ "for i,row in ipairs(result) do \n"
				//+ "  redis.call('rpush', logKey, 'projecting all vars') \n"
				+ "  for j,val in ipairs(row) do \n"
				+ "    if string.find(val, '^#') then \n"
				+ "      row[j] = getLiteralFromAlias(val) \n"
				+ "    end \n"
				+ "  end \n"
				+ "  result[i] = row \n"
				+ "end \n"
				+ " \n"
				+ "-- put the jsonified result table into inputGraphPatternKey \n"
				+ "redis.call('rpush', logKey, 'Final (translated) result is ' .. cjson.encode(result)) \n"
				+ "redis.call('rpush', graphPatternKey, cjson.encode(result)) \n"
				+ "-- we're done \n"
				+ "return 1 \n"
				+ "";
		
	}
	
	public void cleanup(){
	
	}
	
	
	public void addTriple(Triple t){
		triples.add(t);
	}

	@Override
	public String execute(ShardedRedisTripleStore ts, String keyspace, String graphPatternKey) {
		String bgpMatchScriptHandle = ts.loadScript(bgpMatchScript);
		
		String tripleKey = keyspace + "bgp:working:triples";
		String logKey = keyspace + "bgp:log";
		// put json-encoded triples into first key
		for(Triple t: triples){
			JSONArray ja = new JSONArray();
			ja.put(ts.getAlias(t.getSubject()));
			ja.put(ts.getAlias(t.getPredicate()));
			ja.put(ts.getAlias(t.getObject()));
			String json = ja.toString();
			for(Jedis j: ts.shards){
				j.rpush(tripleKey, json);
			}
		}
		
		
		
		
		ArrayList<Object> results = new ArrayList<Object>();
		for(Jedis j: ts.shards){
			results.add(j.evalsha(bgpMatchScriptHandle, 3, tripleKey, graphPatternKey, logKey));
		}
		
		return graphPatternKey;
		
	}


}
