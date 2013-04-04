package translate.redis;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;

import redis.clients.jedis.Jedis;

import com.hp.hpl.jena.graph.Triple;

import main.ShardedRedisTripleStore;

public class Project implements RedisOP {
	List<String> projectedVariables;
	String projectScript;
	
	public Project(){
		projectedVariables = new ArrayList<String>();
		projectScript = ""
				+ "  \n"
				+ "local graphPatternKey = KEYS[1] \n"
				+ "local projectedVarsKey = KEYS[2] \n"
				+ "local logKey = KEYS[3] \n"
				+ "local projectedVarNames = cjson.decode(redis.call('get', projectedVarsKey)) \n"
				+ "\n"
				+ "local graphPatternLen = redis.call('llen', graphPatternKey) \n"
				+ "for graphIdx = 0, graphPatternLen - 1, 1 do \n"
				+ "  local result = cjson.decode(redis.call('lindex', graphPatternKey, graphIdx)) \n"
				+ "  -- figure out variable projection \n"
				+ "  local projectAllVars = false \n" 
				+ "  local projectedVarIndexes = {} \n"
				+ "  if projectedVarNames[1] == '*' then \n"
				+ "    projectAllVars = true \n"
				+ "  else \n"
				+ "    for i,projectedVarName in ipairs(projectedVarNames) do \n"
				+ "      for j,outputVarName in ipairs(result[1]) do \n"
				+ "        if outputVarName == projectedVarName then \n"
				+ "          table.insert(projectedVarIndexes, j) \n"
				+ "        end \n"
				+ "      end \n"
				+ "    end \n"
				+ "  end \n"
				+ "  -- do the projection \n"
				+ "  for i,row in ipairs(result) do \n"
				+ "    if projectAllVars then \n"
				+ "      -- no-op \n"
				+ "    else \n"
				+ "      local newRow = {} \n"
				+ "      for i,projectedIdx in ipairs(projectedVarIndexes) do \n"
				+ "        table.insert(newRow, row[projectedIdx]) \n"
				+ "      end \n"
				+ "      result[i] = newRow \n"
				+ "    end \n"
				+ "  end \n"
				+ "   \n"
				+ "  -- put the jsonified result table into graphPatternKey \n"
				+ "  redis.call('lset', graphPatternKey, graphIdx, cjson.encode(result)) \n"
				+ "end \n"
				+ "-- we're done \n"
				+ "return 1 \n"
				+ "";
		
	}
	@Override
	public String execute(ShardedRedisTripleStore ts, String keyspace,
			String graphPatternKey) {
		String projectScriptHandle = ts.loadScript(projectScript);
		
		String projectedVarsKey = keyspace + "bgp:working:projectedVars";
		String logKey = keyspace + "bgp:log";
		// put projected variables into second key
		
		for(Jedis j: ts.shards){
			j.set(projectedVarsKey, new JSONArray(projectedVariables).toString());
		}
		
		
		
		ArrayList<Object> results = new ArrayList<Object>();
		for(Jedis j: ts.shards){
			results.add(j.evalsha(projectScriptHandle, 3, graphPatternKey, projectedVarsKey, logKey));
		}
		
		return graphPatternKey;
	}

	public void projectVariable(String name) {
		projectedVariables.add('?' + name);
		
	}

}
