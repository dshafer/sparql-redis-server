package translate.redis;

import java.util.ArrayList;
import java.util.Set;
import java.util.Stack;


import com.hp.hpl.jena.sparql.expr.Expr;


public class MapPhaseOrder implements RedisOP{
	
	RedisExpressionVisitor rev;
	RedisOP parent;
	public MapPhaseOrder(RedisOP _parent, RedisExpressionVisitor _rev){
		parent = _parent;
		rev = _rev;
	}
	
	@Override
	public String mapLuaScript() {
		
		StringBuilder result = new StringBuilder(parent.mapLuaScript());
		
		result.append( 
			  "  \n"
			+ "log('map -> Order') \n"
			+ "local rowScoreFuncBody = \"return function(vars) return (" + rev.getLuaFunctionExpression() + ") end\" \n"
			+ "log('compiling: ' .. rowScoreFuncBody) \n"
			+ "local rowScoreFunc = loadstring(rowScoreFuncBody)() \n"
			+ "-- add the sort column \n"
			+ "\n"
			+ "for i,mapResult in ipairs(mapResults) do\n"
			+ "  local newResult = {} \n"
			//+ "  table.insert(newResult, mapResult[1]) \n"
			+ "  local varNames = mapResult[1] \n"
			+ "  local varNames = mapResult[1] \n"
			+ "  local sortKeyIdx = #varNames + 1 \n"
			+ "  for i,row in ipairs(mapResult) do \n"
			+ "    if not (i == 1) then \n"
			+ "      local vars = {} \n"
			+ "      for vi,varName in ipairs(varNames) do \n"
			+ "        log('row is ' .. cjson.encode(row)) \n"
			+ "        log('vi is ' .. cjson.encode(vi)) \n"
			+ "        if string.find(row[vi], '^!') then \n"
			+ "          vars[varName] = string.sub(row[vi], 2, -1) \n"
			+ "        else \n"
			+ "          vars[varName] = row[vi] \n"
			+ "        end"
			+ "      end \n"
			+ "      log('vars is ' .. cjson.encode(vars)) \n"
			+ "      table.insert(row, rowScoreFunc(vars)) \n"
			+ "      table.insert(newResult, row) \n"
			+ "    end \n"
			+ "  end \n"
			+ "   \n"
			+ "  local sortFn = function(a,b) return a[sortKeyIdx] < b[sortKeyIdx] end \n"
			+ "  table.sort(newResult, sortFn) \n"
			+ "  mapResults[i] = {} \n"
			+ "  table.insert(varNames, 'META_SORT_ASC') \n"
			+ "  table.insert(mapResults[i], varNames) \n"
			+ "  for ii, row in ipairs(newResult) do \n"
			+ "  	table.insert(mapResults[i], row) \n"
			+ "  end \n"
			+ "  log('sorted is ' .. cjson.encode(mapResults[i])) \n"
			+ "end \n"
			+ "");
		return result.toString();
		
	}

	@Override
	public Boolean completeAfterMapPhase() {
		return true;
	}

	@Override
	public String toString(String indent) {
		StringBuilder sb = new StringBuilder();
		sb.append("MapPhaseOrder{\n");
		sb.append(indent + "  luaFunc: " + rev.getLuaFunctionExpression() + ",\n");
		sb.append(indent + "  parent : ");
		sb.append(parent.toString(indent + "    ") + "\n");
		sb.append(indent + "}");
		return sb.toString();
	}

	@Override
	public RedisOP tryConvertToJoin(RedisOP op, Boolean left) {
		RedisOP attempt = parent.tryConvertToJoin(op, left);
		if(attempt != null){
			parent = attempt;
			return this;
		} else {
			return null;
		}
	}

	@Override
	public Boolean tryAddFilter(RedisExpressionVisitor rev) {
		return parent.tryAddFilter(rev);
	}

	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		// TODO
		return parent.reduce(patternStack);
	}

	@Override
	public Set<String> getJoinVariables() {
		return parent.getJoinVariables();
	}
	
}