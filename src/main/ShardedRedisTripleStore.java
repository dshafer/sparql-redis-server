package main;

import java.util.List;
import java.util.ArrayList;
import java.util.Stack;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import translate.redis.QueryResult;
import translate.sparql.SPARQLRedisVisitor;

import java.io.UnsupportedEncodingException;
import java.security.*;

import main.DataTypes.GraphResult;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

public class ShardedRedisTripleStore {
	Jedis aliasDb;
	public List<Jedis> shards;
	int numShards;
	String insertTripleHandle;
	public ShardedRedisTripleStore(JedisShardInfo aliasDbInfo, List<JedisShardInfo> tripleDbInfos) {
		aliasDb = new Jedis(aliasDbInfo);
		
		shards = new ArrayList<Jedis>();
		numShards = 0;
		for (JedisShardInfo shard : tripleDbInfos){
			Jedis j = new Jedis(shard);
			
			shards.add(j);
			numShards++;
		}
		
		// ARGV[1] = subject alias
		// ARGV[2] = predicate alias
		// ARGV[3] = object alias or literal
		// ARGV[4] = 1 if object is literal, 0 otherwise
		String insertTripleScript = ""
				+ "local function getLiteralAlias(l) \n"
				//+ "  l = string.sub(l, 2, -2) \n"
				+ "  if redis.call('hexists', 'literalAliases', l) == 1 then \n"
				+ "    return redis.call('hget', 'literalAliases', l) \n"
				+ "  end \n"
				+ "  local lAlias = '#' .. redis.call('hlen', 'literalAliases') \n"
				+ "  redis.call('hset', 'literalAliases', l, lAlias) \n"
				+ "  redis.call('hset', 'literalLookup', lAlias, l) \n"
				+ "  return lAlias \n"
				+ "end \n"
				+ "local subjectAlias = ARGV[1] \n"
				+ "local predicateAlias = ARGV[2] \n"
				+ "local objectAliasOrLiteral = ARGV[3] \n"
				+ "local objectIsLiteral = ARGV[4] \n"
				+ "local objectAlias = (objectIsLiteral == '1') and getLiteralAlias(objectAliasOrLiteral) or objectAliasOrLiteral \n"
				+ "local Spo = 'S:' .. subjectAlias \n"
				+ "local sPo = 'P:' .. predicateAlias \n"
				+ "local spO = 'O:' .. objectAlias \n"
				+ "local SPo = 'SP:' .. subjectAlias .. ':' .. predicateAlias \n"
				+ "local SpO = 'SO:' .. subjectAlias .. ':' .. objectAlias \n"
				+ "local sPO = 'PO:' .. predicateAlias .. ':' .. objectAlias \n"
				+ "redis.call('sadd', Spo, predicateAlias .. ':' .. objectAlias) \n"
				+ "redis.call('sadd', sPo, subjectAlias .. ':' .. objectAlias) \n"
				+ "redis.call('sadd', spO, subjectAlias .. ':' .. predicateAlias) \n"
				+ "redis.call('sadd', SPo, objectAlias) \n"
				+ "redis.call('sadd', SpO, predicateAlias) \n"
				+ "redis.call('sadd', sPO, subjectAlias) \n"
				+ "";
		
		for(Jedis db: shards){
			insertTripleHandle = db.scriptLoad(insertTripleScript);
		}
	}
	
	public void flushdb(){
		aliasDb.flushDB();
		for(Jedis db:shards){
			db.flushDB();
		}
	}
	
	public void insertTriple(Triple t){
		insertTriple(t.getSubject(), t.getPredicate(), t.getObject());
	}
	
	public void insertTriple(Node s, Node p, Node o){
		// figure out which shard we're storing this in
		Jedis db = shards.get(calcShardIdx(s.toString(true)));
		
		// get the aliases for each piece
		String sA = getAlias(s);
		String sP = getAlias(p);
		String sO = getAlias(o);
		
		db.evalsha(insertTripleHandle, 0, sA, sP, sO, o.isLiteral() ? "1" : "0");
		
//		// set the value for forward-lookup (lookup-by-subject)
//		db.rpush("f:" + sP + ":" + sA, sO);
//		if (!o.isLiteral()){
//			// set the value for the reverse-lookup (lookup-by-predicate)
//			db.rpush("r:" + sP + ":" + sO, sA);
//		}
	}
	
	
	public String getStringFromAlias(String alias){
		if(alias.startsWith("!")){
			// this is a literal.  just strip the "!" and return
			return alias.substring(1);
		} else {
			// this is a URI alias
			return getUriNounFromAlias(aliasDb, alias);
		}
	}
	
	public String getAlias(Node n){
		Jedis db = aliasDb;
		if(n.isURI()){
			return getUriAlias(db, n);
		} else if (n.isLiteral()) {
			return getLiteralAlias(db, n);
		} else {
			return n.toString();
			//return shorten(db, "node", n.toString(true));
		}
	}
	private String calcAlias(long idx){
		String chars = "abcdefghijklmnopqrstuvxwyz012345";
		String result = "";
		long remainder = idx % 32;
		idx /= 32;
		result += chars.charAt((int)remainder);
		while (idx != 0){
			remainder = idx % 32;
			idx /= 32;
			result += chars.charAt((int)remainder);
		}
		return result;
	}
	
	private String getUriAlias(Jedis db, Node n){
		return shorten(db, "uri", n.toString());
	}
	private String getUriNounFromAlias(Jedis db, String a){
		return unshorten(db, "uri", a);
	}
	
	private String getLiteralAlias(Jedis db, Node n){
		
		return n.getLiteralValue().toString();
	}
	
	
	private String shorten(Jedis db, String type, String noun) {
		String key = "alias:" + type;
		String alias = db.hget(key, noun);
		if (alias == null){
			alias = calcAlias(db.hlen(key));
			db.hset(key, noun, alias);
			db.hset(key + "_r", alias, noun);
		}
		return alias;
	}
	
	private String unshorten(Jedis db, String type, String alias){
		String key = "alias:" + type + "_r";
		return db.hget(key, alias);
	}
	
	// keeping this dumb and simple for now.
	private int calcShardIdx(String key){
		byte[] bytesOfMessage;
		try {
			bytesOfMessage = key.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			return 0;
		}
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			return 0;
		}
		byte[] digest = md.digest(bytesOfMessage);
		
		// we'll just use the first 2 bytes to calculate the shard index for simplicity
		int lowOrder = digest[0] + (256 * digest[1]);
		return Math.abs(lowOrder % numShards);
	}

	public String loadScript(String script) {
		String result = null;
		for(Jedis db:shards){
			result = db.scriptLoad(script);
		}
		// TODO Auto-generated method stub
		return result;
	}
	
	public QueryResult execute(SPARQLRedisVisitor v){
		// run map phase
		String luaMapScript = v.luaMapScript();
		for (Jedis db: shards){
			db.eval(luaMapScript, 2, "mapResults", "log");
		}
		
		// for each returned graph pattern, union the results from all nodes
		List<List<String>> rawResults = new ArrayList<List<String>>();
		// rawResults is indexed by [shard][patternIdx]
		for (Jedis db: shards){
			rawResults.add(db.lrange("mapResults", 0, -1));
		}
		Stack<QueryResult> patternStack = new Stack<QueryResult>();
		
		int patternIdx = rawResults.get(0).size() - 1;
				
		while(patternIdx > -1){
			QueryResult qr = new QueryResult();
			for(int shardIdx = 0; shardIdx < rawResults.size(); shardIdx++){
				qr.addPatternFromJSON(rawResults.get(shardIdx).remove(patternIdx));
			}
			patternStack.push(qr);
			patternIdx -= 1;
		}
		
		QueryResult result = v.QueryOP().reduce(patternStack);
		return result;
	}
}
