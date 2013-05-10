package com.drewshafer.sparql.backend.redis;

import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Tuple;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.drewshafer.sparql.Options;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.BaseDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ShardedRedisTripleStoreV2 extends ClusteredJedis implements ShardedRedisTripleStore {
	final static Boolean DEBUG_PROFILE = false;
	final AtomicInteger queuedThreads;
	
	String bulkInsertTripleHandle;
	String insertTripleHandle;
	ShardedRedisAliasDb aliasDb;
	
	static final int MAX_PENDING_BULK_INSERTS = 10;
	static final int BULK_INSERT_BLOCK_SIZE_PER_SHARD = 100;
	final int BULK_INSERT_BLOCK_SIZE;
	
	static public final PrefixMapping prefixMapping;
	static public String hostname;
	static {
		prefixMapping = PrefixMapping.Factory.create();
		prefixMapping.setNsPrefix("xsd", "http://www.w3.org/2001/XMLSchema#");
		prefixMapping.setNsPrefix("bsbm", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/");
		Process proc;
		try {
			proc = Runtime.getRuntime().exec("hostname");
			BufferedReader stdIn = new BufferedReader(new InputStreamReader(proc.getInputStream()));
	
	        // read the output from the command
			String s = stdIn.readLine();
		    stdIn.close();
			hostname = s.replace("\n", "");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			hostname="unknown";
		}


	}
	
	private ExecutorService mapExecutor;
	Options options;
	
	public ShardedRedisTripleStoreV2(Options _options, String redisCmd, List<JedisShardInfo> aliasDbInfo, List<JedisShardInfo> tripleDbInfos) {
		super("TripleDb", redisCmd, _options.redisBaseConfig, _options.dataDir, tripleDbInfos, tripleDbInfos.size());
		this.options = _options;
		BULK_INSERT_BLOCK_SIZE = BULK_INSERT_BLOCK_SIZE_PER_SHARD * numShards;
		queuedThreads = new AtomicInteger(0);
		
		if(!options.testParse){
			aliasDb = new ShardedRedisAliasDb(redisCmd, _options, _options.redisBaseConfig, _options.dataDir, aliasDbInfo);
			mapExecutor = null;
			
			if(_options.startRedisServers){
				super.startDatabases();
			}
			
			
			
			String bulkInsertTripleScript = ""
					+ "local function getLiteralAlias(l) \n"
					+ "  l = cjson.encode(l) \n"
					+ "  if redis.call('hexists', 'literalAliases', l) == 1 then \n"
					+ "    return redis.call('hget', 'literalAliases', l) \n"
					+ "  end \n"
					+ "  local lAlias = '#' .. redis.call('hlen', 'literalAliases') \n"
					+ "  redis.call('hset', 'literalAliases', l, lAlias) \n"
					+ "  redis.call('hset', 'literalLookup', lAlias, l) \n"
//					+ "  redis.call('rpush', 'log', 'getLiteralAlias: changed ' .. l .. ' into ' .. lAlias) \n"
					+ "  return lAlias \n"
					+ "end \n"
					+ "local tripleList = redis.call('lrange', KEYS[1], '0', '-1') \n"
//					+ "redis.call('rpush', 'log', 'input is: ' .. cjson.encode(tripleList)) \n"
					+ "for i,tripleJson in ipairs(tripleList) do"
//					+ "  redis.call('rpush', 'log', 'got triple ' .. tripleJson) \n"
					+ "  local triple = cjson.decode(tripleJson) \n"
					+ "  local subjectAlias = triple[1] \n"
					+ "  local predicateAlias = triple[2] \n"
					+ "  local objectAliasOrLiteral = triple[3] \n"
					+ "  local objectIsLiteral = triple[4] \n"
					+ "  local objectAlias = (objectIsLiteral == '1') and getLiteralAlias(objectAliasOrLiteral) or objectAliasOrLiteral \n"
					+ "  local encodedValue = cjson.encode({ subjectAlias, predicateAlias, objectAlias}) \n"
					+ "  redis.call('sadd', 'S:' .. subjectAlias, encodedValue) \n"
					+ "  redis.call('sadd', 'P:' .. predicateAlias, encodedValue) \n"
					+ "  if objectIsLiteral == '0' then \n"
					+ "    redis.call('sadd', 'O:' .. objectAlias, encodedValue) \n"
					+ "  end \n"
//					+ "  redis.call('rpush', 'log', 'finished with triple ' .. i) \n"
					+ "end \n"
					+ "";
			
			bulkInsertTripleHandle = loadScript(bulkInsertTripleScript);
		}
	}
	
	@Override
	public void startDatabases(){
		if(!options.testParse){
			super.startDatabases();
			aliasDb.startDatabases();
		}
	}
	
	@Override
	public void flushdb(){
		if(!options.testParse){
			aliasDb.flushdb();
			super.flushdb();
		}
	}
	
	@Override
	public void killThreads(){
		if(!options.testParse){
			super.killThreads();
			aliasDb.killThreads();
		}
	}

	@Override
	public String dbInfo(){
		StringBuilder result = new StringBuilder();
		result.append("*****************\n");
		result.append("* ALIAS DB INFO *\n");
		result.append("*****************\n");
		result.append(aliasDb.dbInfo());
		
		result.append("\n*****************\n");
		result.append("* Triple DB INFO *\n");
		result.append("*****************\n");
		result.append(super.dbInfo());
		
		return result.toString();
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
	
	final AtomicInteger loadedTripleCount = new AtomicInteger(0);
	final int PRINT_PROGRESS_GRANULARITY = 100000;
	final AtomicInteger nextPrintThreshold = new AtomicInteger();
	public void loadFromFile(String filename){
		final ExecutorService loadExecutor = Executors.newFixedThreadPool(numShards);
		try {
			nextPrintThreshold.set(PRINT_PROGRESS_GRANULARITY);
			final AtomicInteger pendingBulkLoadBlocks = new AtomicInteger(0);
			final class TripleBulkLoader implements Runnable {
				final List<String> lines;
				public TripleBulkLoader(List<String> lines) {
					while(pendingBulkLoadBlocks.get() > MAX_PENDING_BULK_INSERTS){
						try {
							Thread.sleep(1);
						} catch (InterruptedException e) {
							int x = 0;
							//pass
						}
					}
					this.lines = lines;
					pendingBulkLoadBlocks.incrementAndGet();
				}
				
				@Override
				public void run() {
					Set<Node> nodes = new HashSet<Node>(lines.size() * 3);
					List<Triple> triplesToInsert = new ArrayList<Triple>(lines.size());
					
					// parse
					for(String line : lines){
						try{
							int firstSpace = line.indexOf(' ');
							int secondSpace = line.indexOf(' ', firstSpace + 1);
							int eol = line.indexOf(" .", secondSpace + 1);
							String s = line.substring(1,  firstSpace-1);
							String p = line.substring(firstSpace + 2, secondSpace -1);
							String o = line.substring(secondSpace + 1, eol);
							
							Node sN = Node.createURI(s);
							Node pN = Node.createURI(p);
							nodes.add(sN);
							nodes.add(pN);
							Node oN = parseLiteral(o);
							if(oN == null){
								oN = Node.createURI(o.substring(1, o.length()-1));
								nodes.add(oN);
							}
							triplesToInsert.add(new Triple(sN, pN, oN));
						}
						catch(Exception e){
							System.err.println("Parsing error on line: " + line);
							System.exit(1);
						}

					}
					
					if(!options.testParse){
					
						// get aliases
						Map<Node, String> aliases = aliasDb.makeAliases(nodes);
						
						// build a structure to direct the triples to their destination
						// indexed by shardLookup[shardDest][n][subject, predicate, object, objectIsLiteral]
						final List<List<String>> shardLookup = new ArrayList<List<String>>();
						for(int sIdx=0; sIdx < numShards; sIdx++){
							shardLookup.add(new ArrayList<String>());
						}
						
						// convert the triples to string arrays and slot them up for bulk insert
						for(Triple t : triplesToInsert){
							Node s = t.getSubject();
							Node p = t.getPredicate();
							Node o = t.getObject();
						
							String sAlias = aliases.get(s);
							String pAlias = aliases.get(p);
							
							String oAlias;
							boolean objectIsLiteral = o.isLiteral();
							String objectIsLiteralS = objectIsLiteral ? "1" : "0";
							boolean doObjectInsert;
							int sIndex = calcShardIdx(s.toString());
							int oIndex = -1;
							String objectQuote = "\"";
							if(!objectIsLiteral){
								oAlias = aliases.get(o);
								oIndex = calcShardIdx(o.toString());
								doObjectInsert = oIndex != sIndex;
							} else {
								objectQuote = "";
								doObjectInsert = false;
								oAlias = getLiteralAlias(o);
							}
							
							String tripleString = "[\"" + sAlias + "\",\"" + pAlias + "\"," + objectQuote + oAlias + objectQuote + ",\"" + objectIsLiteralS + "\"]";
							shardLookup.get(sIndex).add(tripleString);
							if(doObjectInsert){
								shardLookup.get(oIndex).add(tripleString);
							}
						}
						
						final class RedisTripleBulkInsertRunnable implements Runnable {
							final int shardIdx;
							public RedisTripleBulkInsertRunnable(int shardIdx){
								this.shardIdx = shardIdx;
							}
							@Override
							public void run() {
								Jedis db = shards.get().get(shardIdx);
								if(db != null){
									String argKey = claimUniqueKey(db);
									for(String tripleString : shardLookup.get(shardIdx)){
										db.rpush(argKey, tripleString);
									}
									//db.sync();
									db.evalsha(bulkInsertTripleHandle, 1, argKey);
									releaseUniqueKey(db, argKey);
									
								}
							}
						}
						
						List<Runnable> tripleInsertTasks = new ArrayList<Runnable>(numShards);
						for(int sIdx = 0; sIdx < numShards; sIdx++){
							tripleInsertTasks.add(new RedisTripleBulkInsertRunnable(sIdx));
						}
						runInParallelAndWait(tripleInsertTasks);
					}
					pendingBulkLoadBlocks.decrementAndGet();
					int lTC = loadedTripleCount.addAndGet(lines.size());
					if(lTC >= nextPrintThreshold.get()){
						System.out.println(hostname + ": loadedTripleCount is " + lTC);
						nextPrintThreshold.addAndGet(PRINT_PROGRESS_GRANULARITY);
					}
				}
				
			}
			
			System.out.println("Loading triples from " + filename);
			BufferedReader br = null;
			try {
				br = new BufferedReader(new FileReader(filename));
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			String line;
			try {
				List<String> lineBlock = new ArrayList<String>(BULK_INSERT_BLOCK_SIZE);
				while ((line = br.readLine()) != null) {
					lineBlock.add(line);
					if(lineBlock.size() == BULK_INSERT_BLOCK_SIZE){
						TripleBulkLoader loader = new TripleBulkLoader(lineBlock);
						loadExecutor.execute(loader);
	//					loader.run();
						lineBlock = new ArrayList<String>(BULK_INSERT_BLOCK_SIZE);
					}
				}
				if(lineBlock.size() > 0){
					TripleBulkLoader loader = new TripleBulkLoader(lineBlock);
					loadExecutor.execute(loader);
					loader.run();
					lineBlock = new ArrayList<String>(BULK_INSERT_BLOCK_SIZE);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			
			System.out.println("Finished inserting - waiting for threadpool to catch up");
			while(pendingBulkLoadBlocks.get() > 0){
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					//pass
				}
			}
			this.waitForBackgroundThreads();
			if(!options.testParse){
				aliasDb.waitForBackgroundThreads();
			}
			
			System.out.println("Loaded " + loadedTripleCount.get() + " triples.");
		}
		finally {
			loadExecutor.shutdown();
		}
	}
	
	

	
	public String getAliasOrLiteralValue(Node n){
		if(n.isURI()){
			return aliasDb.getUriAlias(n);
		} else if (n.isLiteral()) {
			return n.getLiteralLexicalForm();
		} else {
			return n.toString();
		}
	}
	
	public String getAlias(Node n){
		if(n.isURI()){
			return aliasDb.getUriAlias(n);
		} else if (n.isLiteral()) {
			return getLiteralAlias(n);
		} else {

			return n.toString();
		}
	}

	private String getLiteralAlias(Node n){
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
	


	
	public QueryResult execute(SPARQLRedisVisitor v){
		
		if(mapExecutor == null){
			mapExecutor = Executors.newFixedThreadPool(numShards);
		}
		long startTime = System.currentTimeMillis();
		// run map phase
		final String luaMapScript;
		if(DEBUG_PROFILE){
			luaMapScript = this.luaDebugHeader() + this.luaScriptBoilerplate() + v.luaMapScript();
		} else {
			luaMapScript = this.luaScriptBoilerplate() + v.luaMapScript();
		}
//		System.out.println("Script >>>>>>>>>");
//		System.out.println(luaMapScript);
//		System.out.println("<<<<<<<<<<<<");
		String keySpace = ""; //UUID.randomUUID().toString();
		final String mapKey = "mapResults:" + keySpace;
		final String logKey = "log:" + keySpace;
		
		final List<List<QueryResult>> _rawResults = new ArrayList<List<QueryResult>>();
		final AtomicInteger taskCount = new AtomicInteger(0);
		final List<List<String>> logs = new ArrayList<List<String>>();
		final class JedisRunnable implements Runnable {

			final int shardIndex;
			public JedisRunnable(int shardIndex){
				this.shardIndex = shardIndex;
			}
			@Override
			public void run() {
				Jedis db = shards.get().get(shardIndex);
				if(db != null){
					db.del(logKey);
					db.del(mapKey);
					db.eval(luaMapScript, 2, mapKey, logKey);
					
					List<QueryResult> rList = new ArrayList<QueryResult>();
					String result = db.lpop(mapKey);
					while(result != null){
						QueryResult qR = new QueryResult();
						qR.addPatternFromJSON(result);
						rList.add(qR);
						result = db.lpop(mapKey);
					}
					_rawResults.set(shardIndex, rList);
					db.del(logKey);
					db.del(mapKey);
				}
				taskCount.decrementAndGet();
			}
		}
		
		for	(int sIdx=0; sIdx < numShards; sIdx++){
			taskCount.incrementAndGet();
			_rawResults.add(null);
			logs.add(null);
			JedisRunnable jr = new JedisRunnable(sIdx);
			mapExecutor.execute(jr);
		}
		
		while(taskCount.get() > 0){
			Thread.yield();
		}
		System.out.println("Lua script execution: " + (double)(System.currentTimeMillis() - startTime)/1000 + " seconds");
		
		if(DEBUG_PROFILE){
			for(Tuple t: shards.get().get(0).zrevrangeWithScores("line_sample_count", 0, 20)){
				System.out.println("line# " + t.getElement() + " : " + t.getScore());
			}
		}
		startTime = System.currentTimeMillis();
		
		Stack<QueryResult> patternStack = new Stack<QueryResult>();
		
		int patternIdx = 0;
		
		startTime = System.currentTimeMillis();
		System.out.println(_rawResults.get(0).size() + " patterns returned");
		while(patternIdx < _rawResults.get(0).size()){
			List<QueryResult> pieces = new ArrayList<QueryResult>();
			for(int shardIdx = 0; shardIdx < _rawResults.size(); shardIdx++){
				pieces.add(_rawResults.get(shardIdx).get(patternIdx));
			}
			QueryResult merged = merge(pieces);
			patternStack.push(merged);
			patternIdx += 1;
		}
		System.out.println("Merge: " + (double)(System.currentTimeMillis() - startTime)/1000 + " seconds");
		
		startTime = System.currentTimeMillis();
		QueryResult result = v.QueryOP().reduce(patternStack);
		System.out.println("Reduce: " + (double)(System.currentTimeMillis() - startTime)/1000 + " seconds");
		if(patternStack.isEmpty()){
			System.out.println("all patterns consumed");
		} else {
			System.out.println("Error! " + patternStack.size() + " patterns remaining after reduce!");
		}
		System.out.println("");
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
	
	public String luaDebugHeader(){
		return ""
				+ "redis.call('del', 'line_sample_count') \n"
				+ "local dbgCount = 0 \n"
				+ "local dbgCountEn = 1 \n"
				+ "local function profile() \n"
				+ "  local line = debug.getinfo(2)['currentline'] \n"
				+ "  redis.call('zincrby', 'line_sample_count', 1, line) \n"
				+ "  if dbgCountEn == 1 then \n"
				+ "    dbgCount = dbgCount + 1"
				+ "  end \n"
				+ "  if dbgCount > 10000000 then \n"
				+ "    local tblVar = {} \n"
				+ "    log('dying now...') \n"
				+ "    log('blah' .. tblVar) \n"
				+ "  end \n"
				+ "end \n"
				+ "debug.sethook(profile, '', 100) \n"
				+ "";
	}
	
	public String luaScriptBoilerplate(){
	  StringBuilder sb = new StringBuilder();
	  sb.append(""

				+ " \n"
				

				
				+ "local function _log(s) \n"
				+ "  local logKey = KEYS[2] \n"
				+ "  redis.call('rpush', logKey, s) \n"
				+ "end \n"				
				+ "local function clearLog() \n"
				+ "  local logKey = KEYS[2] \n"
				+ "  redis.call('del', logKey) \n"
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
				
				+ "local function _split(pString, pPattern) \n"
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
				
				+ "local function decodeTriples(headerRow, tJson) \n"
				+ "  local result = {headerRow} \n"
				+ "  for i,pattern in ipairs(tJson) do \n"
				+ "    table.insert(result, cjson.decode(pattern)) \n"
				+ "  end \n"
				+ "  return result \n"
				+ "end \n"
				
				+ "local function getLiteralFromAlias(alias) \n"
				+ "  return cjson.decode(redis.call('hget', 'literalLookup', alias)) \n"
				+ "end \n"
				


				
			  );
	  return sb.toString();
	}

	@Override
	public Map<String, Node> getNodesFromAliases(Set<String> aliases) {
		return aliasDb.getValuesFromAliases(aliases);
	}
}
