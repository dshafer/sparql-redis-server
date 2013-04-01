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
				+ "local scratchKey = KEYS[3] \n"
				+ "local logKey = KEYS[4] \n"
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
				+ "local searchFuncs = {} \n"
				+ "local alteredVars = {} \n"
				+ "while redis.call('llen', tripleListKey) > 0 do \n"
				+ "  redis.call('rpush', logKey, 'Entering while loop') \n"
				+ "  local boundVars = {} \n"
				+ "  -- find out what vars are being supplied \n"
				+ "  local inputVarList = cjson.decode(redis.call('lpop', graphPatternKey)) \n"
				+ "  local inputVarMap = { } \n"
				+ "  for i,varName in ipairs(inputVarList) do \n"
				+ "    inputVarMap[varName] = i \n"
				+ "  end\n"
				+ "  \n"
				+ "  local triplePatternJson = redis.call('lpop', tripleListKey) \n"
				+ "  redis.call('rpush', logKey, 'Processing Triple pattern' .. triplePatternJson) \n"
				+ "  local triplePattern = cjson.decode(triplePatternJson) \n"
				+ "  -- triplePattern is an array of [s, p, o] \n"
				+ "  \n"
				+ "  local unbound = {} \n"
				+ "  local bound = {} \n"
				+ "  local outputVarList = {} \n"
				+ "  for i, varName in ipairs(inputVarList) do \n"
				+ "    table.insert(outputVarList, varName) \n"
				+ "  end \n"
				+ "  local searchCtrl = {'S', 'P', 'O'} \n"
				+ "  local searchType = '' \n"
				+ "  for i,pattern in ipairs(triplePattern) do \n"
				+ "     unbound[i] = string.find(pattern, '^%?') and (not inputVarMap[pattern])\n"
				+ "     bound[i] = string.find(pattern, '^%?') and inputVarMap[pattern]\n"
				+ "     if unbound[i] then \n"
				+ "       table.insert(outputVarList, pattern) \n"
				+ "     else \n"
				+ "       searchType = searchType .. searchCtrl[i] \n"
				+ "     end \n"
				+ "  end \n"
				+ "  redis.call('rpush', logKey, 'Found bound/unbound vars') \n"
				+ "  -- save output variable list to scratch key \n"
				+ "  redis.call('rpush', scratchKey, cjson.encode(outputVarList)) \n"
				+ "  -- track all new variables this triple populates from predicate and object \n"
				+ "  for i = 2,3 do \n"
				+ "    if unbound[i] then \n"
				+ "      alteredVars[triplePattern[i]] = true \n"
				+ "    end \n"
				+ "  end \n"
				+ "  -- iterate over existing graph pattern \n"
				+ "  while redis.call('llen', graphPatternKey) > 0 do \n"
				+ "    local oldVals = redis.call('lpop', graphPatternKey) \n"
				+ "    local values = split(oldVals, ':') \n"
				+ "    if not (oldVals == '') then \n"
				+ "      oldVals = oldVals .. ':' \n"
				+ "    end \n"
				+ "    local valDict = {} \n"
				+ "    for i,val in ipairs(values) do \n"
				+ "      valDict[inputVarList[i]] = val \n"
				+ "    end \n"
				+ "    local thisTriple = {} \n"
				+ "    local thisSearchKey = searchType \n"
				+ "    for i,val in ipairs(triplePattern) do \n"
				+ "      if not unbound[i] then \n"
				+ "        if bound[i] then \n"
				+ "          thisSearchKey = thisSearchKey .. ':' .. valDict[val] \n"
				+ "        else \n"
				+ "          thisSearchKey = thisSearchKey .. ':' .. val \n"
				+ "        end \n"
				+ "      end \n"
				+ "    end \n"
				+ "    redis.call('rpush', logKey, 'thisSearchKey is ' .. thisSearchKey) \n"
				+ "    local smembers = redis.call('smembers', thisSearchKey) \n"
				+ "    for i,newVals in ipairs(smembers) do \n"
				+ "      redis.call('rpush', scratchKey, oldVals .. newVals) \n"
				+ "    end \n"
				+ "  end \n"
				+ "  redis.call('rename', scratchKey, graphPatternKey) \n "
				+ "  -- return 1 \n"
				+ "end \n"
				+ "";
		
	}
	
	public void cleanup(){
	
	}
	
	
	public void addTriple(Triple t){
		triples.add(t);
		
	}

	@Override
	public GraphResult execute(ShardedRedisTripleStore ts, String graphPatternKey, String scratchKey) {
		String bgpMatchScriptHandle = ts.loadScript(bgpMatchScript);
		
		String tripleKey = "bgp:working:triples";
		String logKey = "bgp:log";
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
			results.add(j.evalsha(bgpMatchScriptHandle, 4, tripleKey, graphPatternKey, scratchKey, logKey));
		}
		
		// TODO Auto-generated method stub
		return null;
	}
}
