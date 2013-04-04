package translate.redis;

import java.util.ArrayList;

import org.json.JSONArray;

import redis.clients.jedis.Jedis;
import main.ShardedRedisTripleStore;

public class Distinct implements RedisOP {

	String distinctScript;
	
	public Distinct(){
		distinctScript = ""
				+ "  \n"
				+ "local graphPatternKey = KEYS[1] \n"
				+ "local logKey = KEYS[2] \n"
				+ "\n"
				+ "local graphPatternLen = redis.call('llen', graphPatternKey) \n"
				+ "for graphIdx = 0, graphPatternLen - 1, 1 do \n"
				+ "  local pattern = cjson.decode(redis.call('lindex', graphPatternKey, graphIdx)) \n"
				+ "  local newPattern = {}"
				+ "  local seen = {} \n"
				+ "  for i, row in ipairs(pattern) do \n"
				+ "    local rowJson = cjson.encode(row) \n"
				+ "    if not seen[rowJson] then \n"
				+ "      table.insert(newPattern, row) \n"
				+ "      seen[rowJson] = true \n"
				+ "    end \n"
				+ "  end \n"
				+ "  -- put the jsonified result table into graphPatternKey \n"
				+ "  redis.call('lset', graphPatternKey, graphIdx, cjson.encode(newPattern)) \n"
				+ "end \n"
				+ "-- we're done \n"
				+ "return 1 \n"
				+ "";
		
	}


	
	@Override
	public String execute(ShardedRedisTripleStore ts, String keyspace,
			String graphPatternKey) {
		String distinctScriptHandle = ts.loadScript(distinctScript);
		
		ArrayList<Object> results = new ArrayList<Object>();
		for(Jedis j: ts.shards){
			results.add(j.evalsha(distinctScriptHandle, 2, graphPatternKey, keyspace + "distinct:log"));
		}
		
		return graphPatternKey;
	}

}
