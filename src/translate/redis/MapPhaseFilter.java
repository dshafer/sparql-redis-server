package translate.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.json.JSONArray;
import org.json.JSONObject;

import redis.clients.jedis.Jedis;

import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import com.hp.hpl.jena.sparql.expr.ExprFunction0;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.ExprFunction3;
import com.hp.hpl.jena.sparql.expr.ExprFunctionN;
import com.hp.hpl.jena.sparql.expr.ExprFunctionOp;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.ExprVisitor;
import com.hp.hpl.jena.sparql.expr.NodeValue;

import main.ShardedRedisTripleStore;

public class MapPhaseFilter implements RedisOP{
	
	class LuaFilter implements ExprVisitor {
		
		public String luaExpression;
		public String getLuaFunctionExpression(){
			return luaExpression;
			//return "return function(vars) return " + luaExpression + " end \n"; 
		}
		public LuaFilter (){
			luaExpression = "";
		}

		@Override
		public void startVisit() {
			System.out.println("startVisit");
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(ExprFunction0 func) {
			System.out.println("ExprFunction0: " + func);
			throw new UnsupportedOperationException();
		}

		@Override
		public void visit(ExprFunction1 func) {
			System.out.println("ExprFunction1: " + func);
			throw new UnsupportedOperationException();
		}

		@Override
		public void visit(ExprFunction2 func) {
			System.out.println("ExprFunction2: " + func);
			System.out.println("  op  : " + func.getOpName());
			Boolean numeric = true;
			
			String opStr = func.getOpName();
			if(opStr == "!="){
				luaExpression += " not ";
				opStr = "==";
			} else if (opStr == "||"){
				opStr = "or";
			}
			luaExpression += " ( ";
			if(numeric){
				luaExpression += "tonumber";
			}
			luaExpression += "(";
			func.getArg1().visit(this);
			luaExpression += ") ";
			luaExpression += opStr + " ";
			if(numeric){
				luaExpression += "tonumber";
			}
			luaExpression += "(";
			func.getArg2().visit(this);
			luaExpression += ") ";
			luaExpression += " ) ";
		}

		@Override
		public void visit(ExprFunction3 func) {
			System.out.println("ExprFunction3: " + func);
			throw new UnsupportedOperationException();
		}

		@Override
		public void visit(ExprFunctionN func) {
			System.out.println("ExprFunctionN: " + func);
			throw new UnsupportedOperationException();
		}

		@Override
		public void visit(ExprFunctionOp funcOp) {
			System.out.println("ExprFunctionOp: " + funcOp);
			throw new UnsupportedOperationException();
		}

		@Override
		public void visit(NodeValue nv) {
			System.out.println("NodeValue: " + nv);
			luaExpression += nv.asUnquotedString();
		}

		@Override
		public void visit(ExprVar nv) {
			System.out.println("ExprVar: " + nv);
			luaExpression += "vars['?" + nv.getVarName() + "']";
		}

		@Override
		public void visit(ExprAggregator eAgg) {
			System.out.println("ExprAggregator: " + eAgg);
			throw new UnsupportedOperationException();
		}

		@Override
		public void finishVisit() {
			System.out.println("finishVisit");
			// TODO Auto-generated method stub
			
		}
		
	}

	List<Expr> expressions;
	String filterScript;
	RedisOP parent;
	public MapPhaseFilter(RedisOP _parent){
		parent = _parent;
		expressions = new ArrayList<Expr>();
		filterScript = ""
				+ "  \n"
				+ "local graphPatternKey = KEYS[1] \n"
				+ "local filterFuncBody = KEYS[2] \n"
				+ "local logKey = KEYS[3] \n"
				+ "redis.call('rpush', logKey, 'compiling: ' .. redis.call('get', filterFuncBody)) \n"
				+ "local filterFunc = loadstring(redis.call('get', filterFuncBody))() \n"
//				+ "if err then \n"
//				+ "  redis.call('rpush', logKey, err) \n"
//				+ "end \n"
				+ "\n"
				+ "local graphPatternLen = redis.call('llen', graphPatternKey) \n"
				+ "for graphIdx = 0, graphPatternLen - 1, 1 do \n"
				+ "  local result = cjson.decode(redis.call('lindex', graphPatternKey, graphIdx)) \n"
				+ "  local newResult = {} \n"
				+ "  table.insert(newResult, result[1]) \n"
				+ "  local varNames = result[1] \n"
				+ "  for i,row in ipairs(result) do \n"
				+ "    if not (i == 1) then \n"
				+ "      local vars = {} \n"
				+ "      for vi,varName in ipairs(varNames) do \n"
				+ "        if string.find(row[vi], '^!') then \n"
				+ "          vars[varName] = string.sub(row[vi], 2, -1) \n"
				+ "        else \n"
				+ "          vars[varName] = row[vi] \n"
				+ "        end"
				+ "      end \n"
				+ "      redis.call('rpush', logKey, 'vars is ' .. cjson.encode(vars)) \n"
				+ "      -- redis.call('rpush', logKey, 'func return ' .. filterFunc(vars)) \n"
				+ "      if filterFunc(vars) then \n"
				+ "        redis.call('rpush', logKey, '  passed filter') \n"
				+ "        table.insert(newResult, row) \n"
				+ "      end \n"
				+ "    end \n"
				+ "  end \n"
				+ "   \n"
				+ "  -- put the jsonified result table into graphPatternKey \n"
				+ "  redis.call('lset', graphPatternKey, graphIdx, cjson.encode(newResult)) \n"
				+ "end \n"
				+ "-- we're done \n"
				+ "return 1 \n"
				+ "";
		
	}
	
	private String getFilterJSON(Expr e){
		JSONObject obj = new JSONObject();
		return null;
	}
	
	@Override
	public String mapLuaScript() {
		
		String result =  parent.mapLuaScript();
		StringBuilder sb = new StringBuilder();
		sb.append("return function(vars) return ");
		String delimeter = "";
		for(Expr e: expressions){
			LuaFilter lf = new LuaFilter();
			e.visit(lf);
			sb.append(delimeter + lf.getLuaFunctionExpression());
			delimeter = "&&";
		}
		sb.append(" end ");
		result += 
			  "  \n"
			+ "log('map -> Filter') \n"
			+ "local filterFuncBody = \"" + sb.toString() + "\" \n"
			+ "log('compiling: ' .. filterFuncBody) \n"
			+ "local filterFunc = loadstring(filterFuncBody)() \n"
			+ "\n"
			+ "for i,mapResult in ipairs(mapResults) do\n"
			+ "  local newResult = {} \n"
			+ "  table.insert(newResult, mapResult[1]) \n"
			+ "  local varNames = mapResult[1] \n"
			+ "  for i,row in ipairs(mapResult) do \n"
			+ "    if not (i == 1) then \n"
			+ "      local vars = {} \n"
			+ "      for vi,varName in ipairs(varNames) do \n"
			+ "        if string.find(row[vi], '^!') then \n"
			+ "          vars[varName] = string.sub(row[vi], 2, -1) \n"
			+ "        else \n"
			+ "          vars[varName] = row[vi] \n"
			+ "        end"
			+ "      end \n"
			+ "      log('vars is ' .. cjson.encode(vars)) \n"
			+ "      if filterFunc(vars) then \n"
			+ "        log('  passed filter') \n"
			+ "        table.insert(newResult, row) \n"
			+ "      end \n"
			+ "    end \n"
			+ "  end \n"
			+ "   \n"
			+ "  mapResults[i] = newResult \n"
			+ "end \n"
			+ "";
		return result;
		
//		String filterScriptHandle = ts.loadScript(filterScript);
//		
//		String filterFuncBodyKey = keyspace + "filter:funcBody";
//		
//		for(Expr e: expressions){
//			LuaFilter lf = new LuaFilter();
//			e.visit(lf);
//			String fExpr = lf.getLuaFunctionExpression();
//			//fExpr = "return function(vars) return cjson.encode(vars) end";
//			for(Jedis j: ts.shards){
//				j.set(filterFuncBodyKey,  fExpr);
//			}
//			
//			ArrayList<Object> results = new ArrayList<Object>();
//			for(Jedis j: ts.shards){
//				results.add(j.evalsha(filterScriptHandle, 3, graphPatternKey, filterFuncBodyKey, logKey));
//			}
//		}
//		return graphPatternKey;
	}

	public void addFilter(Expr e){
		expressions.add(e);
	}

	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		return parent.reduce(patternStack);
	}

	@Override
	public Boolean completeAfterMapPhase() {
		// TODO Auto-generated method stub
		return true;
	}
	
}
