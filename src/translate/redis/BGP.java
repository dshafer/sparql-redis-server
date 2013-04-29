package translate.redis;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import main.ShardedRedisTripleStore;
import main.DataTypes.GraphResult;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;

import redis.clients.jedis.Jedis;
import translate.redis.RedisOP;
import org.json.*;

public class BGP implements RedisOP {
	Set<String> variables;
	List<RedisExpressionVisitor> filterExpressions;
	Set<String> subjectVariables;
	Set<String> objectVariables;
	List<List<String>> triples;
	int id;
	static int idTracker = 0;
	
	
	public BGP() {
		id = idTracker++;
		triples = new ArrayList<List<String>>();
		subjectVariables = new HashSet<String>();
		objectVariables = new HashSet<String>();
		variables = new HashSet<String>();
		filterExpressions = new ArrayList<RedisExpressionVisitor>();
	}
	
	public void cleanup(){
	
	}
	
	public void addTriple(String s, String p, String o){
		List<String> triple = new ArrayList<String>(3);
		triple.add(s);
		triple.add(p);
		triple.add(o);
		triples.add(triple);
		if(!s.startsWith("!")){
			s = s.startsWith("?") ? s.substring(1) : s;
			variables.add(s);
			subjectVariables.add(s);
		}		
		if(!p.startsWith("!")){
			variables.add(p.substring(1));
		}		
		if(!o.startsWith("!")){
			variables.add(o.substring(1));
			objectVariables.add(o.substring(1));
		}
	}
	
	private String luaTripleString(){
		StringBuilder sb = new StringBuilder();
		String delimiter = "";
		for(List<String> triple: triples){
			String s = triple.get(0);
			String p = triple.get(1);
			String o = triple.get(2);
			String luaTriple = "{'" + s + "'," + "'" + p + "'," + "'" + o + "'}";
			sb.append(delimiter + luaTriple);
			delimiter = ",";
		}
		return sb.toString();
	}

	@Override
	public String mapLuaScript() {
		
		StringBuilder result = new StringBuilder( ""
				+ "log('map -> BGP_" + id + "') \n"
				+ "local tripleList = { " + luaTripleString() + " } \n"
				+ "\n"
//
				+ "\n"
				+ "local searchFuncs = {} \n"
				+ "local alteredVars = {} \n"
				+ "local tripleTables = {} \n"
				+ "local thisMapResult = {} \n"
				+ "local searchCtrl = {'S', 'P', 'O'} \n"
				+ "for ti, triplePattern in ipairs(tripleList) do \n"
				+ "  -- triplePattern is an array of [s, p, o] \n"
				+ "  local searchType = '' \n"
				+ "  local searchKey = '' \n"
				+ "  local outputVarList = {} \n"
				+ "  local outputVarIdx = {} \n"
				+ "  local patternResult = {} \n"
				+ "  local searchKeys = {} \n"
				+ "  for i,pattern in ipairs(triplePattern) do \n"
				+ "    if string.find(pattern, '^%?') then \n"
				+ "      table.insert(outputVarList, pattern) \n"
				+ "      table.insert(outputVarIdx, i) \n"
				+ "    else \n"
				+ "      table.insert(searchKeys, searchCtrl[i] .. ':' .. pattern) \n"
				+ "      table.insert(outputVarList, '?META_JOIN_' .. pattern) \n"
				+ "      table.insert(outputVarIdx, i) \n"
				+ "    end \n"
				+ "  end \n"
				+ "  table.insert(patternResult, outputVarList) \n"
				+ "  local patternMembers = {} \n"
				+ "  if searchKeys[2] then \n"
				+ "    log('  sinter ' .. searchKeys[1] .. ' ' .. searchKeys[2]) \n"
				+ "    patternMembers = redis.call('sinter', searchKeys[1], searchKeys[2]) \n"
				+ "  else \n"
				+ "    log('  smembers ' .. searchKeys[1]) \n"
				+ "    patternMembers = redis.call('smembers', searchKeys[1]) \n"
				+ "  end \n"
				+ "  for i,pattern in ipairs(patternMembers) do \n"
				+ "    local triple = split(pattern, ':') \n"
				+ "    local row = {} \n"
				+ "    for i, vi in ipairs(outputVarIdx) do \n"
				+ "      table.insert(row, triple[vi]) \n"
				+ "    end \n"
				+ "    table.insert(patternResult, row) \n"
				+ "  end \n"
				+ "  table.insert(tripleTables, patternResult) \n"
				+ "end \n"
				+ " \n"
				+ "-- do the joins over the various BGP triples \n"
				+ "thisMapResult = table.remove(tripleTables, 1) \n"
				+ "-- log('initial pattern is ' .. cjson.encode(thisMapResult)) \n"
				+ "local joinCount = 0 \n"
				+ "local extra = table.remove(tripleTables, 1) \n"
				+ "while extra do \n"
				+ "  thisMapResult = naturalJoin(thisMapResult, extra) \n"
				+ "  joinCount = joinCount + 1 \n"
				+ "  extra = table.remove(tripleTables, 1) \n"
				+ "end \n"
				+ " \n"
				+ "-- translate the literals \n"
				+ "for i,row in ipairs(thisMapResult) do \n"
				+ "  for j,val in ipairs(row) do \n"
				+ "    if string.find(val, '^#') then \n"
				+ "      row[j] = getLiteralFromAlias(val) \n"
				+ "    end \n"
				+ "  end \n"
				+ "  thisMapResult[i] = row \n"
				+ "end \n"
				+ " \n"
				+ "\n"
				+ "");
		
		if(filterExpressions.size() > 0){
			String delim = "";
			StringBuilder luaFunc = new StringBuilder();
			for(RedisExpressionVisitor rev: filterExpressions){
				luaFunc.append(delim +  rev.getLuaFunctionExpression());
				delim = " and ";
			}
			result.append(""
					+ "log('BGP_" + id + " -> Filter') \n"
					+ "local filterFuncBody = \"return function(vars) return (" + luaFunc.toString() + ") end\" \n"
					+ "log('compiling: ' .. filterFuncBody) \n"
					+ "local filterFunc = loadstring(filterFuncBody)() \n"
					+ "\n"
					+ "local newResult = {} \n"
					+ "table.insert(newResult, thisMapResult[1]) \n"
					+ "local varNames = thisMapResult[1] \n"
					+ "for i,row in ipairs(thisMapResult) do \n"
					+ "  if not (i == 1) then \n"
					+ "    local vars = {} \n"
					+ "    for vi,varName in ipairs(varNames) do \n"
					+ "      if string.find(row[vi], '^!') then \n"
					+ "        vars[varName] = string.sub(row[vi], 2, -1) \n"
					+ "      else \n"
					+ "        vars[varName] = row[vi] \n"
					+ "      end"
					+ "    end \n"
//					+ "    log('vars is ' .. cjson.encode(vars)) \n"
//					+ "    if filterFunc(vars) then \n"
//					+ "      table.insert(newResult, row) \n"
//					+ "    end"
					+ "    local success, passedFilter = pcall(filterFunc, vars) \n"
					+ "    if not success then \n"
					+ "      log('!!!! bad filter eval.  vars is ' .. cjson.encode(vars)) \n"
					+ "      log('error message: ' .. passedFilter) \n"
					+ "    elseif passedFilter then \n"
					+ "      table.insert(newResult, row) \n"
					+ "    end \n"
					+ "  end \n"
					+ "end \n"
					+ "thisMapResult = newResult \n"
					+ "");
		}
		
		result.append("" 
				+ "table.insert(mapResults, thisMapResult) \n"
				+ "log('################################') \n"
				+ "log('BGP_" + id + " inserted result at index ' .. (#mapResults - 1)) \n"
				+ "log('  headers: ' .. cjson.encode(thisMapResult[1])) \n"
				+ "log('  ' .. (#thisMapResult - 1) .. ' rows') \n"
				+ "-- log('Final (translated) BGP result is ' .. cjson.encode(thisMapResult)) \n"
				+ "log('') \n"
				);
		
		return result.toString();

		
	}

	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		QueryResult result = patternStack.pop();
		Set<String> resultVars = new HashSet<String>();
		for(String colName : result.columnNames){
//			if(!colName.startsWith("?META")){
				resultVars.add(colName.substring(1));
//			}
		}
		if(!resultVars.equals(variables)){
			int x = 0;
			int y = x;
		}
		return result;
	}

	@Override
	public Boolean completeAfterMapPhase() {
		return true;
	}

	@Override
	public String toString(String indent) {
		StringBuilder sb = new StringBuilder();
		sb.append("BGP_" + id + " {\n");
		//String extraIndent = "";
		String delimiter = "";
		if(filterExpressions.size() > 0){
			sb.append(indent + "  filter: {");
			//sb.append(indent + "  expr: (");
			for(RedisExpressionVisitor e: filterExpressions){
				sb.append(delimiter + e.getLuaFunctionExpression());
				delimiter = " and ";
			}
			//extraIndent = "  ";
			sb.append("},\n");
		} 
		sb.append(indent + "  pattern : { ");
		delimiter = "";
		for(List<String> triple : triples){
			String jsonLine = "{'" + triple.get(0) + "','" + triple.get(1) + "','" + triple.get(2) + "'}";
			sb.append(delimiter + jsonLine);
			delimiter = ",";
		}
		sb.append(" }\n");
		
		sb.append(indent + "}");
			
		return sb.toString();
	}

	@Override
	public Boolean tryAddFilter(RedisExpressionVisitor rev) {
		Set<String> varsMentioned = new HashSet<String>();
		for(Var v:rev.getVarsMentioned()){
			varsMentioned.add(v.getVarName());
		}
		if(variables.containsAll(varsMentioned)){
			Map<String, String> equalities = rev.getEqualities();
			for(String varName : equalities.keySet()){
				String varVal = equalities.get(varName);
				for(List<String> triple : triples){
					for(int i = 0; i < 3; i++){
						if(triple.get(i).equals(varName)){
							triple.set(i, varVal);
						}
					}
				}
			}
			if(rev.hasNonEqualities()){
				filterExpressions.add(rev);
			}
			return true;
		} else {
			return false;
		}
			
	}
	
	@Override
	public RedisOP tryConvertToJoin(RedisOP op, Boolean left) {
		if (this.getJoinVariables().containsAll(op.getJoinVariables())){
			return left ? new MapPhaseLeftJoin(this, op) : new MapPhaseJoin(this, op);
		}
		return null;
	}

	@Override
	public Set<String> getJoinVariables() {
		Set<String> result = new HashSet<String>(subjectVariables);
		if(result.size() == 1){
			return result;
		} else {
			result.retainAll(objectVariables);
			return result;
		}
	}

}
