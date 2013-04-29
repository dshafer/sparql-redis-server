package main;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Stack;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Pipeline;
import translate.redis.QueryResult;
import translate.sparql.SPARQLRedisVisitor;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.*;

import org.json.JSONObject;

import main.DataTypes.GraphResult;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.BaseDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_URI;
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
				+ "local encodedValue = subjectAlias .. ':' .. predicateAlias .. ':' .. objectAlias \n"
				+ "redis.call('sadd', 'S:' .. subjectAlias, encodedValue) \n"
				+ "redis.call('sadd', 'P:' .. predicateAlias, encodedValue) \n"
				+ "if not objectIsLiteral then \n"
				+ "  redis.call('sadd', 'O:' .. objectAlias, encodedValue) \n"
				+ "end \n"
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
	
	private Node parseLiteral(String l){
		if(l.startsWith("\"")){
			String lV;
			RDFDatatype dt = null;
			String lang = null;
			if(l.contains("\"^^")){
				int sep = l.indexOf("\"^^");
				lV = l.substring(1, sep);
				String dV = l.substring(sep+3);
				dt = new BaseDatatype(dV);
//				Node result = Node.createLiteral(lV, dt);
//				return result;
			} else {
				if(l.startsWith("\"") && !l.endsWith("\"")){  // for text literals that specify language
					lV = l.substring(1, l.length()-4);
					lang = l.substring(l.length()-2).toUpperCase();
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
					oN = Node.createURI(o);
				}

				insertTriple(sN, pN, oN);
			   // process the line.
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		
		if(o.toString().contains("ProductType66")){
			int x = 0;
			int y = x + 1;
		}
		
		db.evalsha(insertTripleHandle, 0, sA, sP, sO, o.isLiteral() ? "1" : "0");
		
		if(o.isURI()){
			db = shards.get(calcShardIdx(o.toString(true)));
			db.evalsha(insertTripleHandle, 0, sA, sP, sO, "0");
		}
		
	}
	
	
	public String getStringFromAlias(String alias){
		if((alias == null) || alias.startsWith("@")){
			return null;
		} else if(alias.startsWith("!")){
			// this is a literal.  just strip the "!" and return
			return alias.substring(1);
		} else {
			// this is a URI alias
			return getUriNounFromAlias(aliasDb, alias);
		}
	}	
	
	public Node getNodeFromAlias(String alias){
		if((alias == null) || alias.startsWith("@")){
			return null;
		} else if(alias.startsWith("{")){
			// this is a literal.  just strip the "!" and return
			//return alias.substring(1);
			JSONObject j = new JSONObject(alias);
			String lV = j.getString("v");
			RDFDatatype dt = null;
			String lang = null;
			if(j.has("d")) {
				dt = new BaseDatatype(j.getString("d"));
			}
			if(j.has("l")){
				lang = j.getString("l");
			}
			return Node.createLiteral(lV, lang, dt);
		} else {
			// this is a URI alias
			return Node.createURI(getUriNounFromAlias(aliasDb, alias));
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
			sb.append(",\"d\":\"" + n.getLiteralDatatypeURI() + "\"");
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
	
	private class JedisRunnable implements Runnable {

		final Jedis db;
		final String luaScript;
		public JedisRunnable(Jedis _db, String _luaScript){
			db = _db;
			luaScript = _luaScript;
		}
		@Override
		public void run() {
			db.del("log");
			db.del("mapResults");
			db.eval(luaScript, 2, "mapResults", "log");
		}
		
	}
	
	public QueryResult execute(SPARQLRedisVisitor v){

		// run map phase
		String luaMapScript = this.luaScriptBoilerplate() + v.luaMapScript();
		List<Thread> thrs = new ArrayList<Thread>();
		for (Jedis db: shards){
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
		
		
		// for each returned graph pattern, union the results from all nodes
		List<List<String>> rawResults = new ArrayList<List<String>>();
		// rawResults is indexed by [shard][patternIdx]
		for (Jedis db: shards){
			rawResults.add(db.lrange("mapResults", 0, -1));
		}
		Stack<QueryResult> patternStack = new Stack<QueryResult>();
		
		int patternIdx = 0;//rawResults.get(0).size() - 1;
		
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
		
		QueryResult result = v.QueryOP().reduce(patternStack);
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
			String val = null; //pieces.get(0).rows.get(rowPtrs[0]).get(sortIdx);
			int nextPiece = -1;
			for(int x=0; x < pieces.size(); x++){
				if(rowPtrs[x] < pieces.get(x).rows.size()){
					String testVal = pieces.get(x).rows.get(rowPtrs[x]).get(sortIdx);
					if((val == null) || (sortAsc && (testVal.compareTo(val) < 0)) || (!sortAsc && (testVal.compareTo(val) > 0))){
						nextPiece = x;
						val = testVal;
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
	
	private String luaScriptBoilerplate(){
	  StringBuilder sb = new StringBuilder();
	  sb.append(""
//				  "local function hashJoin(left, right, joinCols) \n"
//				+ "  local joinTable = {} \n"
//				+ "  local resultTable = {} \n"
//				+ "  local joinSig = '' \n"
//				+ "  local rightJoinCols = {} \n"
//				+ "  for i, joinCol in ipairs(joinCols) do \n"
//				+ "    rightJoinCols[joinCol[3]] = true \n"
//				+ "  end \n"
//				+ "  local rightKeepCols = {} \n"
//				+ "  for ri,rColName in ipairs(right[1]) do \n"
//				+ "    if not rightJoinCols[ri] then \n"
//				+ "      table.insert(rightKeepCols, ri) \n"
//				+ "    end \n"
//				+ "  end \n"
//				+ "  -- compute a table with the hash of the join keys from the left table \n"
//				+ "  for l, lval in ipairs(left) do \n"
//				+ "    joinSig = '' \n"
//				+ "    for i, joinCol in ipairs(joinCols) do \n"
//				+ "      joinSig = joinSig .. lval[joinCol[2]] \n"
//				+ "    end \n"
////				+ "    log('joinSig/lval is ' .. joinSig .. '/' .. cjson.encode(lval)) \n"
//				+ "    joinTable[joinSig] = lval \n"
//				+ "  end \n"
//				+ "  for r, rval in ipairs(right) do \n"
//				+ "    joinSig = '' \n"
//				+ "    for i, joinCol in ipairs(joinCols) do \n"
//				+ "      joinSig = joinSig .. rval[joinCol[3]] \n"
//				+ "    end \n"
//				+ "    if joinTable[joinSig] then \n"
//				+ "      -- found a good row.  Need to join it together and append the result \n"
//				+ "      local outputRow = {} \n"
//				+ "      for li,lItem in ipairs(joinTable[joinSig]) do \n"
//				+ "        table.insert(outputRow, lItem) \n"
//				+ "      end \n"
//				+ "      for ri,rKeepIndex in ipairs(rightKeepCols) do \n"
//				+ "        table.insert(outputRow, rval[rKeepIndex]) \n"
//				+ "      end \n"
//				+ "      table.insert(resultTable, outputRow) \n"
//				+ "    end \n"
//				+ "  end \n"
//				+ "  return resultTable \n"
//				+ "end \n"
				
 
				
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
				+ "    local outputRow = lval \n"
				+ "    if joinTable[joinSig] then \n"
				+ "      -- has a counterpart in right.  Append the values \n"
				+ "      for i, rval in ipairs(joinTable[joinSig]) do \n"
				+ "        for ri,rKeepIndex in ipairs(rightKeepCols) do \n"
				+ "          table.insert(outputRow, rval[rKeepIndex]) \n"
				+ "        end \n"
				+ "        table.insert(resultTable, outputRow) \n"
				+ "      end \n"
				+ "    elseif isLeftJoin then \n"
				+ "      -- no counterpart in right.  Append 'null' values encoded as'@' \n"
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
//				+ "  log('joining on cols: ' .. cjson.encode(joinCols)) \n"
				+ "  local result =  hashJoin(left,right,joinCols, false) \n"
				+ "  -- log('join result is ' .. cjson.encode(result)) \n"
				+ "  -- log('join result has ' .. (#result - 1) .. ' rows') \n"
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
//				+ "  return '!' .. redis.call('hget', 'literalLookup', alias) \n"
				+ "  return redis.call('hget', 'literalLookup', alias) \n"
				+ "end \n"
				
			  );
	  return sb.toString();
	}
}
