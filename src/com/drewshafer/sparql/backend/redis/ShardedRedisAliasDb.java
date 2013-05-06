package com.drewshafer.sparql.backend.redis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONObject;

import com.drewshafer.sparql.Options;
import com.hp.hpl.jena.datatypes.BaseDatatype;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.shared.PrefixMapping;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;

public class ShardedRedisAliasDb extends ClusteredJedis {
	
	private final String getAliasScript = "" +
			"local keyMap = redis.call('hgetall', KEYS[1]) \n" +
			"redis.call('rpush', 'log', 'initial: ' .. cjson.encode(keyMap)) \n" +
			"local aliasMapKey = 'alias:uri' \n" +
			"local aliasMapRevKey = 'alias:uri_r' \n" +
			"local result = {} \n" +
			"for i = 1, #keyMap, 2 do \n" +
			"  local k = keyMap[i] \n" +
			"  local v = keyMap[i+1] \n" +
			"  redis.call('rpush', 'log', 'k, v is ' .. k .. ',' .. v) \n" +
			"  local alias = redis.call('hget', aliasMapKey, k) \n" +
			"  if not alias then \n" +
			"    alias = '' .. redis.call('hlen', aliasMapKey) \n" +
			"    redis.call('hset', aliasMapKey, k, alias) \n" +
			"    redis.call('hset', aliasMapRevKey, alias, k) \n" +
			"  end \n" +
			"  table.insert(result, k) \n" +
			"  table.insert(result, alias) \n" +
			"  keyMap[k] = alias \n" +
			"end \n" +
			"redis.call('rpush', 'log', 'Returning: ' .. cjson.encode(result)) \n" +
			"return result \n" +
			"";
	
	private final String getValuesFromAliasesScript = "" +
			"local keyMap = redis.call('hgetall', KEYS[1]) \n" +
			"local aliasMapRevKey = 'alias:uri_r' \n" +
			"local result = {} \n" +
			//"for alias, v in pairs(keyMap) do \n" +
			"for i = 1, #keyMap, 2 do \n" +
			"  local alias = keyMap[i] \n" +
			"  local v = keyMap[i+1] \n" +
			"  table.insert(result, alias) \n" +
			"  table.insert(result, redis.call('hget', aliasMapRevKey, alias))\n" +
			"end \n" +
			"return result \n" +
			"";
	
	private String getAliasesHandle;
	private String getValuesFromAliasesHandle;
	
	public ShardedRedisAliasDb(String redisCmd, Options options, String redisBaseConfigFile, String dataDir, List<JedisShardInfo> aliasDbInfos){
		super("AliasDb", redisCmd, redisBaseConfigFile, dataDir, aliasDbInfos, aliasDbInfos.size());
		
		if(options.startRedisServers){
			super.startDatabases();
		}
		getAliasesHandle = loadScript(getAliasScript);
		getValuesFromAliasesHandle = loadScript(getValuesFromAliasesScript);
	}
	

	public Map<Node, String> makeAliases(Set<Node> nodes){
		
		final List<Map<Node, String>> shardLookup = new ArrayList<Map<Node, String>>(numShards);
		final class makeAliasRunnable implements Runnable {
			final int shardIdx;
			public makeAliasRunnable(int shardIdx){
				this.shardIdx = shardIdx;
			}
			@Override
			public void run() {
				Jedis db = shards.get().get(shardIdx);
				if(db != null){
					Map<Node, String> nodeMap = shardLookup.get(shardIdx);
					if(nodeMap.size() > 0){
						Map<String, String> strMap = new HashMap<String, String>(nodeMap.size());
						for(Map.Entry<Node, String> e : nodeMap.entrySet()){
							strMap.put(e.getKey().toString(), "-");
						}
						String argKey = claimUniqueKey(db);
						db.hmset(argKey, strMap);
						List<String> result = (List<String>) db.evalsha(getAliasesHandle, 1, argKey);
						releaseUniqueKey(db, argKey);
				
						for(int x = 0; x < result.size()/2; x++){
							strMap.put(result.get(x*2),  result.get(x*2+1));
						}
						for(Map.Entry<Node, String> e : nodeMap.entrySet()){
							e.setValue(shardIdx + ":" + strMap.get(e.getKey().toString()));
						}
					}
				}
			}
		}
		
		// split the keys to their various shards
		for(int x=0; x < numShards; x++){
			shardLookup.add(new HashMap<Node, String>());
		}
		for(Node node : nodes){
			shardLookup.get(calcShardIdx(node.toString())).put(node, "-");
		}
		
		List<Runnable> tasks = new ArrayList<Runnable>();
		// execute them in parallel
		for(int sIdx=0; sIdx < numShards; sIdx++){
			tasks.add(new makeAliasRunnable(sIdx));
		}
		runInParallelAndWait(tasks);
		
		// merge the resulting maps
		Map<Node, String> result = new HashMap<Node,String>(nodes.size());
		for(int sIdx = 0; sIdx < numShards; sIdx++){
			result.putAll(shardLookup.get(sIdx));
		}
		
//		// now the reverse tables need to get populated
//		final class saveReverseAliasRunnable implements Runnable {
//			final int shardIdx;
//			public saveReverseAliasRunnable(int shardIdx){
//				this.shardIdx = shardIdx;
//			}
//			@Override
//			public void run() {
//				Jedis db = shards.get().get(shardIdx);
//				if(db != null){
//					Map<Node, String> nodeMap = shardLookup.get(shardIdx);
//					Map<String, String> strMap = new HashMap<String, String>(nodeMap.size());
//					for(Map.Entry<Node, String> e : nodeMap.entrySet()){
//						strMap.put(e.getKey().toString(), e.getValue());
//					}
//					String argKey = claimUniqueKey(db);
//					db.hmset(argKey, strMap);
//					db.evalsha(saveReverseAliasHandle, 1, argKey);
//					releaseUniqueKey(db, argKey);
//				}
//			}
//		}
//		
//		// execute them in parallel, but don't wait
//		for(int sIdx=0; sIdx < numShards; sIdx++){
//			tasks.add(new makeAliasRunnable(sIdx));
//		}
//		runInParallelBackgrounded(tasks);
		
		return result;
	}

	
	static public final PrefixMapping prefixMapping;
	
	static {
		prefixMapping = PrefixMapping.Factory.create();
		prefixMapping.setNsPrefix("xsd", "http://www.w3.org/2001/XMLSchema#");
		prefixMapping.setNsPrefix("bsbm", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/");
	}
	
	public Map<String, Node> getValuesFromAliases(Set<String> aliases){
		
		final List<Map<String, Node>> shardLookup = new ArrayList<Map<String, Node>>();
		final class getValuesFromAliasesRunnable implements Runnable {
			final int shardIdx;
			public getValuesFromAliasesRunnable(int shardIdx){
				this.shardIdx = shardIdx;
			}
			@Override
			public void run() {
				Jedis db = shards.get().get(shardIdx);
				if(db != null){
					Map<String, Node> nodeMap = shardLookup.get(shardIdx);
					if(nodeMap.size() > 0){
						Map<String, String> strMap = new HashMap<String, String>(nodeMap.size());
						for(Map.Entry<String, Node> e : nodeMap.entrySet()){
							strMap.put(e.getKey().toString().split(":")[1], "-");
						}
						
						String argKey = claimUniqueKey(db);
						
						db.hmset(argKey, strMap);
						List<String> result = null;
						try{
							result = (List<String>) db.evalsha(getValuesFromAliasesHandle, 1, argKey);
						} catch (Exception ex) {
							System.out.println("error while untranslating aliases: " + ex.getMessage());
							for(Map.Entry<String, Node> e : nodeMap.entrySet()){
								System.out.println(e.getKey().toString());
							}
							System.exit(1);
						}
						releaseUniqueKey(db, argKey);
			
						strMap = new HashMap<String, String>();
						for(int x = 0; x < result.size()/2; x++){
							strMap.put(shardIdx + ":" + result.get(x*2),  result.get(x*2+1));
						}
						for(Map.Entry<String, Node> e : nodeMap.entrySet()){
							e.setValue(Node.createURI(strMap.get(e.getKey())));
						}
					}
				}
			}
		}
		
		// split the keys to their various shards
		for(int x=0; x < numShards; x++){
			shardLookup.add(new HashMap<String, Node>());
		}
		aliases.remove("@");
		for(String alias : aliases){
			String[] parts = alias.split(":");
			int shardIdx = Integer.parseInt(parts[0]);
			shardLookup.get(shardIdx).put(alias, null);
		}
		
		List<Runnable> tasks = new ArrayList<Runnable>();
		// execute them in parallel
		for(int sIdx=0; sIdx < numShards; sIdx++){
			tasks.add(new getValuesFromAliasesRunnable(sIdx));
		}
		runInParallelAndWait(tasks);
		
		// merge the resulting maps
		Map<String, Node> result = new HashMap<String,Node>(aliases.size());
		for(int sIdx = 0; sIdx < numShards; sIdx++){
			result.putAll(shardLookup.get(sIdx));
		}
		
		result.put("@",  Node.createLiteral(""));
		
		return result;
	}
	

	public String getUriAlias(Node n){
		int shardIdx = calcShardIdx(n.toString());
		Jedis db = shards.get().get(shardIdx);
		return shardIdx + ":" + shorten(db, "uri", n.toString());
	}
	
	private String shorten(Jedis db, String type, String noun) {
		String key = "alias:" + type;
		String alias = db.hget(key, noun);
		if (alias == null){
			alias = db.hlen(key).toString();// calcAlias(db.hlen(key));
			db.hset(key, noun, alias);
			db.hset(key + "_r", alias, noun);
		}
		return alias;
	}
}
