package translate.redis;

import java.util.ArrayList;
import java.util.Stack;

import org.json.JSONArray;

import redis.clients.jedis.Jedis;
import main.ShardedRedisTripleStore;

public class MapPhaseDistinct implements RedisOP {

	String distinctScript;
	
	public MapPhaseDistinct(){
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
	public String mapLuaScript() {
		throw new UnsupportedOperationException();
	}



	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public Boolean completeAfterMapPhase() {
		// TODO Auto-generated method stub
		return true;
	}



}
