package com.drewshafer.sparql.backend.redis;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.*;

import org.json.JSONObject;

import com.drewshafer.sparql.Options;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.BaseDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
 
public class ShardedRedisTripleStoreV1 implements ShardedRedisTripleStore {

	AtomicInteger queuedThreads;
	static final int NUM_THREADPOOL_THREADS = 5;
	final JedisShardInfo aliasDbInfo;
	final List<JedisShardInfo> tripleDbInfos;
	final ThreadLocal<Jedis> aliasDb = new ThreadLocal<Jedis>(){
		@Override
		protected Jedis initialValue(){
			System.out.println(" Starting new Jedis aliasDb client");
			Jedis result = new Jedis(aliasDbInfo);
			waitForRedis(result);
			return result;
		}
	};
	final ThreadLocal<List<Jedis>> shards = new ThreadLocal<List<Jedis>>(){
		@Override
		protected List<Jedis> initialValue(){
			List<Jedis> result = new ArrayList<Jedis>();
			for (JedisShardInfo shard : tripleDbInfos){
				Jedis j = null;
				
				if(shard != null){
					System.out.println(" Starting new Jedis client for Triple DB at " + shard.getHost() + ":" + shard.getPort());
					j = new Jedis(shard);
					waitForRedis(j);
				}
				
				result.add(j);
			}
			return result;
		}
	};
	final int numShards;
	String insertTripleHandle;
	String shortenTripleHandle;
	Options options;
	ExecutorService executor;
	
	public static Node vivifyLiteral(String val, String lang, String dt){
		RDFDatatype rDT = null;
		if(dt.equals("xsd:integer")){
			rDT = XSDDatatype.XSDinteger;
		} else {
			if(dt != null) {
				rDT = new BaseDatatype(dt);
			}
		}
		Node result = ResourceFactory.createTypedLiteral(val, rDT).asNode();
		return result;
//		return Node.createLiteral(val, lang, rDT);
	}
	
	static public final PrefixMapping prefixMapping;
	
	static {
		prefixMapping = PrefixMapping.Factory.create();
		prefixMapping.setNsPrefix("xsd", "http://www.w3.org/2001/XMLSchema#");
		prefixMapping.setNsPrefix("bsbm", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/");
	}
	
	public ShardedRedisTripleStoreV1(Options _options, String redisCmd, List<JedisShardInfo> _aliasDbInfo, List<JedisShardInfo> _tripleDbInfos) {
		aliasDbInfo = _aliasDbInfo.get(0);
		tripleDbInfos = _tripleDbInfos;
		this.options = _options;
		String dataDir = options.dataDir;
		String redisBaseConfig = options.redisBaseConfig;
		if(options.startRedisServers){
			if(redisBaseConfig == null){
				redisBaseConfig = "";
			}
			startDatabases(redisCmd, redisBaseConfig, dataDir, aliasDbInfo, tripleDbInfos);
		}
		numShards = _tripleDbInfos.size();
		System.out.println("Waiting for redis servers to become ready");
		System.out.println("  aliasDb...");
		queuedThreads = new AtomicInteger();
		executor = Executors.newFixedThreadPool(NUM_THREADPOOL_THREADS);
		
		System.out.println("ready.");
		
		
		String shortenTripleScript = ""
				+ "local function shorten(noun) \n"
				+ "  if not noun then \n"
				+ "    return '' \n"
				+ "  end \n"
				+ "  local key = 'alias:uri' \n"
				+ "  local alias = redis.call('hget', key, noun) \n"
				+ "  if not alias then \n"
				+ "    alias = '' .. redis.call('hlen', key) \n"
				+ "    redis.call('hset', key, noun, alias) \n"
				+ "    redis.call('hset', key .. '_r', alias, noun) \n"
				+ "  end \n"
				+ "  return alias \n"
				+ "end \n"
				+ "local ss = shorten(ARGV[1]) \n"
				+ "local pp = shorten(ARGV[2]) \n"
				+ "local oo = shorten(ARGV[3]) \n"
				+ "return {ss, pp, oo} \n"
				+ "";
		
		shortenTripleHandle = aliasDb.get().scriptLoad(shortenTripleScript);
		
		
		// ARGV[1] = subject alias
		// ARGV[2] = predicate alias
		// ARGV[3] = object alias or literal
		// ARGV[4] = 1 if object is literal, 0 otherwise
		String insertTripleScript = ""
				+ "local function getLiteralAlias(l) \n"
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
				+ "local encodedValue = cjson.encode({ subjectAlias, predicateAlias, objectAlias}) \n"
				+ "redis.call('sadd', 'S:' .. subjectAlias, encodedValue) \n"
				+ "redis.call('sadd', 'P:' .. predicateAlias, encodedValue) \n"
				+ "if objectIsLiteral == '0' then \n"
				+ "  redis.call('sadd', 'O:' .. objectAlias, encodedValue) \n"
				+ "end \n"
				+ "";
		
		for(Jedis db: shards.get()){
			if(db != null){
				insertTripleHandle = db.scriptLoad(insertTripleScript);
			}
		}
	}
	
	private void waitForRedis(Jedis db){
		Boolean done = false;
		while (!done){
			try {
				db.get("test");
				done = true;
			}
			catch (JedisDataException jde){
				System.err.println("Error: " + jde.getMessage());
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (Exception e){
				System.err.println("Unknown Error: " + e.getMessage());
			}
		}
	}
	
	public String dbInfo(){
		StringBuilder result = new StringBuilder();
		
		result.append("aliasDb info:\n");
		result.append(aliasDb.get().info() + "\n");;
		
		int x = 0;
		for(Jedis j : shards.get()){
			result.append("\nTripleDB[" + x + "]:\n");
			if(j != null){
				result.append(" not active\n");
			} else {
				result.append(j.info() + "\n");
			}
		}
		return result.toString();
	}
	
	private void startDb(String redisCmd, String redisConfig, String dbId, String dataDir, JedisShardInfo si){
		if (si != null){
			StringBuilder cmd = new StringBuilder();
	
			if(!si.getHost().equals("127.0.0.1")){
				cmd.append("ssh " + si.getHost() + " ");
			}
			
			cmd.append(redisCmd);
			cmd.append(" " + redisConfig);
			cmd.append(" --daemonize yes");
			cmd.append(" --port " + si.getPort());
			cmd.append(" --pidfile " + dataDir + "pid_" + dbId);
			
			if(!dataDir.isEmpty()){
				cmd.append(" --dbfilename " + dataDir + "db_" + dbId + ".rdb");
			}
			
			Runtime r = Runtime.getRuntime();
			try {
				r.exec(cmd.toString()).waitFor();
				System.out.println("Started redis-server " + dbId + " on " + si.getHost() + ":" + si.getPort());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	private void startDatabases(String redisCmd, String redisConfig, String dataDir, JedisShardInfo aliasDbInfo, List<JedisShardInfo> tripleDbInfos){
		if(!dataDir.endsWith("/")){
			dataDir += "/";
		}
		try {
			Runtime.getRuntime().exec("mkdir -p " + dataDir).waitFor();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		startDb(redisCmd, redisConfig, "alias", dataDir, aliasDbInfo);
		for(int x = 0; x < tripleDbInfos.size(); x++){
			startDb(redisCmd, redisConfig, "triple_" + x, dataDir, tripleDbInfos.get(x));
		}
	}
	
	public void flushdb(){
		aliasDb.get().flushDB();
		for(Jedis db:shards.get()){
			if(db != null){
				db.flushDB();
			}
		}
	}
	
	private Node parseLiteral(String l){
		//PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
		//PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
		if(l.startsWith("\"")){
			String lV;
			RDFDatatype dt = null;
			String lang = null;
			if(l.contains("\"^^<")){
				int sep = l.indexOf("\"^^<");
				lV = l.substring(1, sep);
				String dV = l.substring(sep+4, l.length()-1);
				dt = new BaseDatatype(dV);
			} else {
				if(l.startsWith("\"") && !l.endsWith("\"")){  // for text literals that specify language
					lV = l.substring(1, l.length()-4);
					lang = l.substring(l.length()-2);
				} else {
					lV = l.substring(1, l.length()-1);
				}
			}
			Node result = Node.createLiteral(lV, lang, dt);
			return result;
		} else {
			return null;
		}
	}
	
	public void loadFromFile(String filename){
		System.out.println("Loading triples from " + filename);
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(filename));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String line;
		int tripleCount = 0;
		try {
			while ((line = br.readLine()) != null) {
				while(queuedThreads.get() > NUM_THREADPOOL_THREADS * 2){
					Thread.sleep(1);
				}
				tripleCount++;
				int firstSpace = line.indexOf(' ');
				int secondSpace = line.indexOf(' ', firstSpace + 1);
				int eol = line.indexOf(" .", secondSpace + 1);
				String s = line.substring(1,  firstSpace-1);
				String p = line.substring(firstSpace + 2, secondSpace -1);
				String o = line.substring(secondSpace + 1, eol);
				
				Node sN = Node.createURI(s);
				Node pN = Node.createURI(p);
				Node oN = parseLiteral(o);
				if(oN == null){
					oN = Node.createURI(o.substring(1, o.length()-1));
				}
				
				insertTriple(sN, pN, oN);
				if(tripleCount % 10000 == 0){
					aliasDb.get().set("message", "loaded " + tripleCount + " triples");
				}

			   // process the line.
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Finished inserting - waiting for threadpool to catch up");
		executor.shutdown();
		while(!executor.isTerminated()){
			try {
				Thread.sleep(0);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		executor = Executors.newFixedThreadPool(NUM_THREADPOOL_THREADS);
		System.out.println("Loaded " + tripleCount + " triples.");
//		if(!options.dataDir.isEmpty()){
//			System.out.println("Saving...");
//			aliasDb.save();
//			for(Jedis db : shards){
//				if(db != null){
//					db.save();
//				}
//			}
//			System.out.println("Done.");
//		}
	}
	
	public void insertTriple(Triple t){
		insertTriple(t.getSubject(), t.getPredicate(), t.getObject());
	}
	
	private class insertTripleRunnable implements Runnable {

		final Node s;
		final Node p;
		final Node o;
		
		public insertTripleRunnable(Node _s, Node _p, Node _o){
			queuedThreads.incrementAndGet();
			s = _s;
			p = _p;
			o = _o;
		}
		@Override
		public void run() {

			try {
				String ss = s.toString();
				String pp = p.toString();
				String oo;
				Boolean oIsLiteral = o.isLiteral();
				if(oIsLiteral){
					List<String> aliases = (List<String>)aliasDb.get().evalsha(shortenTripleHandle, 0, ss, pp);
					ss = aliases.get(0);
					pp = aliases.get(1);
					oo = getLiteralAlias(null, o);
				} else {
					List<String> aliases = (List<String>)aliasDb.get().evalsha(shortenTripleHandle, 0, ss, pp, o.toString());
					ss = aliases.get(0);
					pp = aliases.get(1);
					oo = aliases.get(2);
				}
				
				// figure out which shard we're storing this in
				Jedis db = shards.get().get(calcShardIdx(s.toString(true)));
				if(db != null){
					db.evalsha(insertTripleHandle, 0, ss, pp, oo, oIsLiteral ? "1" : "0");
				}
				if(!oIsLiteral){
					db = shards.get().get(calcShardIdx(o.toString(true)));
					if(db != null){
						db.evalsha(insertTripleHandle, 0, ss, pp, oo, "0");
					}
				}
			}
			catch(Exception e){
				e.printStackTrace();
			}
			finally
			{
				queuedThreads.decrementAndGet();
			}
		}
		
	}
	
	public void insertTriple(Node s, Node p, Node o){
		executor.execute(new insertTripleRunnable(s, p, o));
//		insertTripleRunnable r = new insertTripleRunnable(s, p, o);
//		r.run();
	}
	
	public void _insertTriple(Node s, Node p, Node o){
		String sA = getAlias(s);
		String sP = getAlias(p);
		String sO = getAlias(o);

		// figure out which shard we're storing this in
		Jedis db = shards.get().get(calcShardIdx(s.toString(true)));
		if(db != null){
			db.evalsha(insertTripleHandle, 0, sA, sP, sO, o.isLiteral() ? "1" : "0");
		}
		if(o.isURI()){
			db = shards.get().get(calcShardIdx(o.toString(true)));
			if(db != null){
				db.evalsha(insertTripleHandle, 0, sA, sP, sO, "0");
			}
		}
		
	}
	
	
	public Node getNodeFromAlias(String alias){
		if((alias == null) || alias.startsWith("@")){
			return Node.createLiteral("");
		} else if(alias.startsWith("{")){
			JSONObject j = new JSONObject(alias);
			String lV = j.getString("v");
			RDFDatatype dt = null;
			String lang = null;
			if(j.has("d")) {
				String dataType = prefixMapping.expandPrefix(j.getString("d"));
				dt = new BaseDatatype(j.getString(dataType));
			}
			if(j.has("l")){
				lang = j.getString("l");
			}
			return Node.createLiteral(lV, lang, dt);
		} else {
			// this is a URI alias
			return Node.createURI(getUriNounFromAlias(aliasDb.get(), alias));
		}
	}
	
	public String getAliasOrLiteralValue(Node n){
		Jedis db = aliasDb.get();
		if(n.isURI()){
			return getUriAlias(db, n);
		} else if (n.isLiteral()) {
			return n.getLiteralLexicalForm();
		} else {
			return n.toString();
		}
	}
	
	public String getAlias(Node n){
		Jedis db = aliasDb.get();
		if(n.isURI()){
			return getUriAlias(db, n);
		} else if (n.isLiteral()) {
			return getLiteralAlias(db, n);
		} else {

			return n.toString();
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
		StringBuilder sb = new StringBuilder();
		sb.append("{\"v\":\"" + n.getLiteralLexicalForm() + "\"");
		if(n.getLiteralDatatype() != null){
			String mapped = prefixMapping.shortForm(n.getLiteralDatatypeURI());
			sb.append(",\"d\":\"" + mapped + "\"");
		}
		if(!n.getLiteralLanguage().equals("")){
			sb.append(",\"l\":\"" + n.getLiteralLanguage() + "\"");
		}
		sb.append("}");
		return sb.toString();
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
		for(Jedis db:shards.get()){
			if(db != null){
				result = db.scriptLoad(script);
			}
		}
		// TODO Auto-generated method stub
		return result;
	}
	
	private class JedisRunnable implements Runnable {

		final Jedis db;
		final String luaScript;
		public JedisRunnable(Jedis _db, String _luaScript){
			db = _db;
			luaScript = _luaScript;
		}
		@Override
		public void run() {
			if(db != null){
				db.del("log");
				db.del("mapResults");
				db.eval(luaScript, 2, "mapResults", "log");
			}
		}
		
	}
	
	public QueryResult execute(SPARQLRedisVisitor v){
		long startTime = System.currentTimeMillis();
		// run map phase
		String luaMapScript = this.luaScriptBoilerplate() + v.luaMapScript();
		
		List<Thread> thrs = new ArrayList<Thread>();
		for (Jedis db: shards.get()){
			JedisRunnable jr = new JedisRunnable(db, luaMapScript);
			Thread th = new Thread(jr);
			th.start();
			thrs.add(th);
		}
		for (Thread th : thrs){
			try {
				th.join();
			} catch (InterruptedException e) {
				return null;
			}
		}
		System.out.println("Lua script execution: " + (double)(System.currentTimeMillis() - startTime)/1000 + " seconds");
		
		
		startTime = System.currentTimeMillis();
		// for each returned graph pattern, union the results from all nodes
		List<List<String>> rawResults = new ArrayList<List<String>>();
		// rawResults is indexed by [shard][patternIdx]
		for (Jedis db: shards.get()){
			if(db != null){
				rawResults.add(db.lrange("mapResults", 0, -1));
			}
		}
		System.out.println("Gather data to reducer : " + (double)(System.currentTimeMillis() - startTime)/1000 + " seconds");
		
		Stack<QueryResult> patternStack = new Stack<QueryResult>();
		
		int patternIdx = 0;//rawResults.get(0).size() - 1;
		
		startTime = System.currentTimeMillis();
		while(patternIdx < rawResults.get(0).size()){
			List<QueryResult> pieces = new ArrayList<QueryResult>();
			for(int shardIdx = 0; shardIdx < rawResults.size(); shardIdx++){
				QueryResult qr = new QueryResult();
				qr.addPatternFromJSON(rawResults.get(shardIdx).get(patternIdx));
				pieces.add(qr);
			}
			QueryResult merged = merge(pieces);
			patternStack.push(merged);
			patternIdx += 1;
		}
		System.out.println("Merge: " + (double)(System.currentTimeMillis() - startTime)/1000 + " seconds");
		
		startTime = System.currentTimeMillis();
		QueryResult result = v.QueryOP().reduce(patternStack);
		System.out.println("Reduce: " + (double)(System.currentTimeMillis() - startTime)/1000 + " seconds");
		
		return result;
	}
	
	private QueryResult merge(List<QueryResult> pieces){
		List<String> columnNames = pieces.get(0).columnNames;
		int sortIdx = -1;
		Boolean sortAsc = true;
		for(int x=0; x < columnNames.size(); x++){
			if(columnNames.get(x).equals("META_SORT_ASC")){
				sortIdx = x;
				sortAsc = true;
			} else if (columnNames.get(x).equals("META_SORT_DESC")){
				sortIdx = x;
				sortAsc = false;
			}
		}
		QueryResult result = new QueryResult(pieces.get(0).columnNames);
		
		if(sortIdx == -1){
			for(int x = 1; x < pieces.size(); x++){
				pieces.get(0).append(pieces.get(x));
			}
			return pieces.get(0);
		}
		
		int[] rowPtrs = new int[pieces.size()];
		for(int x=0; x < pieces.size(); x++){
			rowPtrs[x] = 0;
		}
		Boolean rowsRemaining = false;
		for(int x=0; x < pieces.size(); x++){
			rowsRemaining |= pieces.get(x).rows.size() > 0;
		}
		int rowCount = 0;
		while(rowsRemaining){
			rowCount++;
			// find next candidate;
			String val = null;
			int nextPiece = -1;
			for(int x=0; x < pieces.size(); x++){
				if(rowPtrs[x] < pieces.get(x).rows.size()){
					Node testVal = pieces.get(x).rows.get(rowPtrs[x]).get(sortIdx);
					if((val == null) 
							|| (sortAsc && (testVal.getLiteralLexicalForm().compareTo(val) < 0)) 
							|| (!sortAsc && (testVal.getLiteralLexicalForm().compareTo(val) > 0))){
						nextPiece = x;
						val = testVal.getLiteralLexicalForm();
					}
				}
			}
			
			result.addRow(pieces.get(nextPiece).rows.get(rowPtrs[nextPiece]));
			rowPtrs[nextPiece]++;
			
			rowsRemaining = false;
			for(int x=0; x < pieces.size(); x++){
				rowsRemaining |= (rowPtrs[x]+1) < pieces.get(x).rows.size();
			}
		}
		
		
		
		return result;
		
	}
	
	public String luaScriptBoilerplate(){
	  StringBuilder sb = new StringBuilder();
	  sb.append(""

				+ " \n"
				+ "local function log(s) \n"
				+ "  local logKey = KEYS[2] \n"
				+ "  redis.call('rpush', logKey, s) \n"
				+ "end \n"
				+ "\n"
				+ "local mapResultKey = KEYS[1] \n"
				+ "local mapResults = {} \n"
				+ "\n" 
				+ "local function table_copy(t) \n"
				+ "  local t2 = {} \n"
				+ "  for k,v in pairs(t) do \n"
				+ "    t2[k] = v \n"
				+ "  end \n"
				+ "  return t2 \n"
				+ "end \n"
				+ "local function hashJoin(left, right, joinCols, isLeftJoin) \n"
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
				+ "    if not joinTable[joinSig] then \n"
				+ "      joinTable[joinSig] = {} \n"
				+ "    end \n"
				+ "    table.insert(joinTable[joinSig], rval) \n"
				+ "  end \n"
				+ "  for li, lval in ipairs(left) do \n"
				+ "    joinSig = '' \n"
				+ "    for i, joinCol in ipairs(joinCols) do \n"
				+ "      joinSig = joinSig .. lval[joinCol[2]] \n"
				+ "    end \n"
				+ "    if joinTable[joinSig] then \n"
				+ "      -- has counterpart(s) in right.  Append the values \n"
				+ "      for ri, rval in ipairs(joinTable[joinSig]) do \n"
				+ "        local outputRow = table_copy(lval) \n"
				+ "        for ri,rKeepIndex in ipairs(rightKeepCols) do \n"
				+ "          table.insert(outputRow, rval[rKeepIndex]) \n"
				+ "        end \n"
				+ "        table.insert(resultTable, outputRow) \n"
				+ "      end \n"
				+ "    elseif isLeftJoin then \n"
				+ "      -- no counterpart in right.  Append 'null' values encoded as'@' \n"
				+ "      local outputRow = table_copy(lval) \n"
				+ "      for ri,rKeepIndex in ipairs(rightKeepCols) do \n"
				+ "        table.insert(outputRow, '@') \n"
				+ "      end \n"
				+ "      table.insert(resultTable, outputRow) \n"
				+ "    end \n"
				+ "  end \n"
				+ "  return resultTable \n"
				+ "end \n"
				+ " \n"
				
				+ "local function naturalJoin(left, right) \n"
				+ "  -- determine join columns\n"
				+ "  local joinCols = {} \n"
				+ "  for l,lColName in ipairs(left[1]) do \n"
				+ "    for r,rColName in ipairs(right[1]) do \n"
				+ "      if (not (lColName == '*')) and (lColName == rColName) then \n"
				+ "        table.insert(joinCols, {lColName, l, r}) \n"
				+ "        break \n"
				+ "      end \n"
				+ "    end \n"
				+ "  end \n"
				+ "  local result =  hashJoin(left,right,joinCols, false) \n"
				+ "  return result \n"
				+ "end \n" 
				
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
				+ "  return hashJoin(left,right,joinCols, true) \n"
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
				
				+ "local function getLiteralFromAlias(alias) \n"
				+ "  return cjson.decode(redis.call('hget', 'literalLookup', alias)) \n"
				+ "end \n"
				
			  );
	  return sb.toString();
	}

	@Override
	public Map<String, Node> getNodesFromAliases(Set<String> aliases) {
		Map<String, Node> result = new HashMap<String, Node>();
		for(String alias : aliases){
			result.put(alias,  this.getNodeFromAlias(alias));
		}
		return result;
	}

	@Override
	public void killThreads() {
		// TODO Auto-generated method stub
		
	}
}
