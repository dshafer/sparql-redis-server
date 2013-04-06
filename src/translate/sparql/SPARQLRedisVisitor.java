/*
 * (C) Copyright 2011 - Juan F. Sequeda and Daniel P. Miranker
 * Permission to use this code is only granted by separate license 
 */
package translate.sparql;

import java.util.Dictionary;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.Queue;

import main.ShardedRedisTripleStore;
import main.DataTypes.GraphResult;

import redis.clients.jedis.Jedis;
import translate.redis.BGP;
import translate.redis.MapPhaseDistinct;
import translate.redis.MapPhaseFilter;
import translate.redis.MapPhaseLeftJoin;
import translate.redis.MapPhaseProject;
import translate.redis.RedisOP;
import translate.redis.ReducePhaseJoin;


import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.SortCondition;
import com.hp.hpl.jena.sparql.algebra.OpVisitor;
import com.hp.hpl.jena.sparql.algebra.op.OpAssign;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpConditional;
import com.hp.hpl.jena.sparql.algebra.op.OpDatasetNames;
import com.hp.hpl.jena.sparql.algebra.op.OpDiff;
import com.hp.hpl.jena.sparql.algebra.op.OpDisjunction;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpExt;
import com.hp.hpl.jena.sparql.algebra.op.OpExtend;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpGraph;
import com.hp.hpl.jena.sparql.algebra.op.OpGroup;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpLabel;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpList;
import com.hp.hpl.jena.sparql.algebra.op.OpMinus;
import com.hp.hpl.jena.sparql.algebra.op.OpNull;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.algebra.op.OpPath;
import com.hp.hpl.jena.sparql.algebra.op.OpProcedure;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpPropFunc;
import com.hp.hpl.jena.sparql.algebra.op.OpQuad;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.algebra.op.OpReduced;
import com.hp.hpl.jena.sparql.algebra.op.OpSequence;
import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;
import com.hp.hpl.jena.sparql.algebra.op.OpTable;
import com.hp.hpl.jena.sparql.algebra.op.OpTopN;
import com.hp.hpl.jena.sparql.algebra.op.OpTriple;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.core.VarExprList;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;

//import translate.sql.BGP;
//import translate.sql.JOIN;
//import translate.sql.LEFTJOIN;
//import translate.sql.SqlExpr;
//import translate.sql.SqlOP;
//import translate.sql.UNION;

public class SPARQLRedisVisitor implements OpVisitor 
{

	final boolean targetClusterTables = false;
	
	Stack<RedisOP> redisOpStack = new Stack<RedisOP>();
	String baseURI = "";
	int maxNumOfKeys = 0;
	ShardedRedisTripleStore ts;
	
	public SPARQLRedisVisitor(ShardedRedisTripleStore _ts) 
	{
		ts = _ts;
	}
	
	public RedisOP QueryOP(){
		return redisOpStack.peek();
	}
	
	/*
	 * Returns a Lua script that takes two arguments:
	 * KEYS[1]: Redis key to store the final JSON-ified map result
	 * KEYS[2]: Redis key for log entries
	 */
	public String luaMapScript(){
		String result = ""
				+ " \n"
				+ "local function log(s) \n"
				+ "  local logKey = KEYS[2] \n"
				+ "  redis.call('rpush', logKey, s) \n"
				+ "end \n"
				+ "\n"
				+ "local mapResultKey = KEYS[1] \n"
				+ "local mapResults = {} \n"
				+ "\n"
				+ redisOpStack.peek().mapLuaScript()
				+ "\n"
				+ "for i,mapResult in ipairs(mapResults) do \n"
				+ "  redis.call('rpush', mapResultKey, cjson.encode(mapResult)) \n"
				+ "end  \n"
				//+ "redis.call('set', mapResultKey, cjson.encode(mapResults)) \n"
				+ "";
		
		return result;
	}
	
//	private String _execute(ShardedRedisTripleStore ts, String keyspace, String graphPatternKey, String logKey){
//		RedisOP op = redisOpStack.pop();
//		if (!redisOpStack.isEmpty()){
//			graphPatternKey = _execute(ts, keyspace, graphPatternKey, logKey);
//		}
//		return op.mapLuaScript(ts, keyspace, graphPatternKey, logKey);
//	}
//	
//	public String execute(ShardedRedisTripleStore ts){
//		String keyspace = "redisSparql:";
//		String graphPatternKey = keyspace + "working:graphResult";
//		String logKey = keyspace + "log";
//		// make sure the initial graph pattern input is empty
//		for(Jedis j: ts.shards){
//			j.del(graphPatternKey);
//		}
//		if (!redisOpStack.isEmpty()){
//			return _execute(ts, keyspace, graphPatternKey, logKey);
//		}
//		return null;
//	}
	
	public void visit(OpProject arg0) 
	{
		System.out.println(">>>> project = " + arg0.getVars());
		MapPhaseProject pOp = new MapPhaseProject();
		for (Var v : arg0.getVars()) 
		{
			pOp.projectVariable(v.getName());
		}
		this.redisOpStack.push(pOp);
	}

	
	public void visit(OpBGP bgp) 
	{
		System.out.println(">>>> bgp = " + bgp.getPattern() );
		Map<String, BGP> subPatterns = new HashMap<String, BGP>();
		for (Triple t : bgp.getPattern()) 
		{
			String s = ts.getAlias(t.getSubject());
			String p = ts.getAlias(t.getPredicate());
			String o = ts.getAlias(t.getObject());
			if (subPatterns.get(s) == null){
				subPatterns.put(s,  new BGP());
			}
			subPatterns.get(s).addTriple(s, p, o);
		}
		
		Stack<RedisOP> patternStack = new Stack<RedisOP>();
		for(BGP b : subPatterns.values()){
			patternStack.push(b);
		}
		
		RedisOP actualOP = null;
		if (patternStack.size() == 1){
			// simple case - don't need to do any reduce-phase joins
			actualOP = patternStack.pop();
		} else {
			actualOP = new ReducePhaseJoin(patternStack.pop(), patternStack.pop());
			while (!patternStack.isEmpty()){
				actualOP = new ReducePhaseJoin(actualOP, patternStack.pop());
			}
		}
		
		this.redisOpStack.push(actualOP);
	}
	
	public void visit(OpReduced arg0) 
	{
		System.out.println(">>>> reduced");
		throw new UnsupportedOperationException();
	}
	
	public void visit(OpDistinct arg0) 
	{
		System.out.println(">>>> distinct");
		throw new UnsupportedOperationException();
	}

	public void visit(OpFilter arg0) 
	{
		System.out.println(">>>> filter = " + arg0.getExprs());
		MapPhaseFilter f = new MapPhaseFilter(redisOpStack.peek());
		for(Expr e: arg0.getExprs()) 
		{
			System.out.println(">>>>>> expr = " + e);
			f.addFilter(e);
		}
		this.redisOpStack.push(f);
	}

	public void visit(OpJoin arg0) 
	{
		System.out.println(">>>> join = " + arg0 );
//		SqlOP rhs = sqlOpStack.pop();
//		SqlOP lhs = sqlOpStack.pop();
//		sqlOpStack.push(new JOIN(++joinIndex, lhs, rhs));
	}
	
	public void visit(OpLeftJoin arg0) 
	{
		System.out.println(">>>> leftjoin = " + arg0 );
		MapPhaseLeftJoin lj = new MapPhaseLeftJoin();
		redisOpStack.push(lj);
		// Filters passed to the left join constructor are included
		// in the 'ON' clause of the left-join operator.
		// Filters added using the Query.addFilter method are added 
		// to the WHERE clause of the left join query.
		//List<SqlExpr> filters = new LinkedList<SqlExpr>();
		
		if (arg0.getExprs() != null) 
		{
			throw new UnsupportedOperationException("Left Join Filters Not Yet Implemented");
//			this.visit((OpFilter)OpFilter.filter(arg0.getExprs(), null));
			
//			for(Expr e: arg0.getExprs()) 
//			{
//				SqlExpr expr = new SqlExpr(baseURI);
//				e.visit(expr);
//				filters.add(expr);
//			}
		}
		
//		SqlOP rhs = sqlOpStack.pop();
//		SqlOP lhs = sqlOpStack.pop();
//		sqlOpStack.push(new LEFTJOIN(++joinIndex, lhs, rhs, filters));
	}
	
	public void visit(OpUnion arg0) 
	{
		System.out.println(">>>> union  = " + arg0 );
//		SqlOP rhs = sqlOpStack.pop();
//		SqlOP lhs = sqlOpStack.pop();
//		sqlOpStack.push(new UNION(++joinIndex, lhs, rhs));
	}
	
	public void visit(OpOrder arg0) 
	{
		System.out.println(">>>> order  = " + arg0 );
//		for (SortCondition c : arg0.getConditions()) 
//		{
//			SqlExpr expr = new SqlExpr(baseURI);
//			c.getExpression().visit(expr);
//			sqlOpStack.peek().orderBy(expr, c.direction != Query.ORDER_DESCENDING);
//		}
	}
	
	public void visit(OpGroup arg0) 
	{
		System.out.println(">>>> group  = " + arg0 );
		
//		List<ExprAggregator> aggregators = arg0.getAggregators();
//		
//		
//		for(ExprAggregator e: aggregators) 
//		{
//			SqlExpr expr = new SqlExpr(baseURI);
//			e.getExpr().visit(expr);
//			sqlOpStack.peek().addAggregator(expr);
//		}
//		
//		//Adding the GROUP BY
//		VarExprList	varExprList = arg0.getGroupVars();
//		List<Var> varList = varExprList.getVars();
//		
//		for(Var v: varList)
//		{
//			sqlOpStack.peek().addGroupBy(v);
//		}
	}
	
	public void visit(OpExtend arg0) 
	{
		System.out.println(">>>> extend  = " + arg0 );
//		VarExprList varExprList = arg0.getVarExprList();
//		
//		Iterator<Entry<Var, Expr>> it = varExprList.getExprs().entrySet().iterator();
//	    while (it.hasNext()) {
//	        Entry<Var, Expr> pairs = it.next();
//	        String keyVarName = pairs.getKey().getVarName();
//	        Expr valueExpression = pairs.getValue();
//	        
//	        
//	        SqlExpr expr = new SqlExpr(baseURI);
//        	valueExpression.visit(expr);
//        	//System.out.println("expr = "+expr);
//        	sqlOpStack.peek().addExtend(keyVarName, expr);
//	        
//	    }
	}
	


	public void visit(OpSlice arg0) 
	{
		System.out.println(">>>> slice  = " + arg0 );
		try {
			//sqlOpStack.peek().slice(arg0.getStart(), arg0.getLength());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	public String toString() 
	{
		return "blah";
		//throw new UnsupportedOperationException("toString not implemented");
	}
	
	
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	public void visit(OpQuadPattern arg0) 
	{
		throw new UnsupportedOperationException("OpQuadPattern Not implemented");
	}
	
	public void visit(OpTriple arg0) 
	{
		throw new UnsupportedOperationException("OpTriple Not implemented");
	}
	
	public void visit(OpPath arg0) 
	{
		throw new UnsupportedOperationException("OpPath Not implemented");
	}

	public void visit(OpTable arg0) 
	{
		throw new UnsupportedOperationException("OpTable Not implemented");
		
	}
	
	public void visit(OpNull arg0) 
	{
		throw new UnsupportedOperationException("OpNull Not implemented");
	}
	
	public void visit(OpProcedure arg0) 
	{
		throw new UnsupportedOperationException("OpProcedure Not implemented");
	}
	
	public void visit(OpPropFunc arg0) 
	{
		throw new UnsupportedOperationException("OpPropFunc Not implemented");
	}
	
	public void visit(OpGraph arg0) 
	{
		throw new UnsupportedOperationException("OpGraph Not implemented");
	}
	
	public void visit(OpService arg0) 
	{
		throw new UnsupportedOperationException("OpService Not implemented");
	}
	
	public void visit(OpDatasetNames arg0) 
	{
		throw new UnsupportedOperationException("OpDatasetNames Not implemented");
	}
	
	public void visit(OpLabel arg0) 
	{
		throw new UnsupportedOperationException("OpLabel Not implemented");
	}

	public void visit(OpAssign arg0) 
	{
		throw new UnsupportedOperationException("OpAssign Not implemented");
	}
	
	public void visit(OpDiff arg0) 
	{
		throw new UnsupportedOperationException("OpDiff Not implemented");
	}
	
	public void visit(OpMinus arg0) 
	{
		throw new UnsupportedOperationException("OpMinus Not implemented");
	}
	
	public void visit(OpConditional arg0) 
	{
		throw new UnsupportedOperationException("OpConditional Not implemented");
	}
	
	public void visit(OpSequence arg0) 
	{
		throw new UnsupportedOperationException("OpSequence Not implemented");
	}

	public void visit(OpDisjunction arg0) 
	{
		throw new UnsupportedOperationException("OpDisjunction Not implemented");
	}
	
	public void visit(OpExt arg0) 
	{
		throw new UnsupportedOperationException("OpExt Not implemented");
	}
	
	public void visit(OpList arg0) 
	{
		throw new UnsupportedOperationException("OpList Not implemented");
	}

	public void visit(OpQuad opQuad) 
	{
		throw new UnsupportedOperationException("OpQuad Not implemented");
	}

	public void visit(OpTopN opTop) 
	{
		throw new UnsupportedOperationException("OpTopN Not implemented");
	}
}