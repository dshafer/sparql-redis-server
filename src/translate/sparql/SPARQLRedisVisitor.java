/*
 * (C) Copyright 2011 - Juan F. Sequeda and Daniel P. Miranker
 * Permission to use this code is only granted by separate license 
 */
package translate.sparql;

import java.util.ArrayList;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.Queue;

import main.ShardedRedisTripleStore;
import main.DataTypes.GraphResult;

import redis.clients.jedis.Jedis;
import translate.redis.BGP;
import translate.redis.ExpressionOptimizer;
import translate.redis.Distinct;
import translate.redis.MapPhaseFilter;
import translate.redis.MapPhaseJoin;
import translate.redis.MapPhaseLeftJoin;
import translate.redis.MapPhaseOrder;
import translate.redis.MapPhaseProject;
import translate.redis.MapPhaseUnion;
import translate.redis.RedisExpressionVisitor;
import translate.redis.RedisFilterOP;
import translate.redis.RedisJoinOP;
import translate.redis.RedisOP;
import translate.redis.RedisProjectOP;
import translate.redis.ReducePhaseFilter;
import translate.redis.ReducePhaseJoin;
import translate.redis.ReducePhaseLeftJoin;
import translate.redis.ReducePhaseOrder;
import translate.redis.ReducePhaseProject;
import translate.redis.ReducePhaseSlice;

import com.hp.hpl.jena.graph.Node;
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
import com.hp.hpl.jena.sparql.expr.ExprList;

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
	int opId;
	
	public SPARQLRedisVisitor(ShardedRedisTripleStore _ts) 
	{
		ts = _ts;
		opId = 0;
	}
	
	@Override
	public String toString(){
		return QueryOP().toString("");
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
				+ "";
		
		return result;
	}
	
	
	public void visit(OpProject arg0) 
	{
		RedisOP parentOp = this.redisOpStack.peek();
		RedisProjectOP projectOp = null;
		if (redisOpStack.peek().completeAfterMapPhase()){
			projectOp = new MapPhaseProject(parentOp);
		} else {
			projectOp = new ReducePhaseProject(parentOp);
		}
		for (Var v : arg0.getVars()) 
		{
			projectOp.projectVariable(v.getName());
		}
		this.redisOpStack.push(projectOp);
	}

	
	public void visit(OpBGP bgp) 
	{
		Set<String> subjectVariables = new HashSet<String>();
		List<List<String>> aliasedTriples = new ArrayList<List<String>>();
		for (Triple t : bgp.getPattern()) 
		{
			Node sn = t.getSubject();
			Node pn = t.getPredicate();
			Node on = t.getObject();
			String s = ts.getAlias(sn);
			String p = ts.getAlias(pn);
			String o = ts.getAlias(on);
			if(sn.isVariable()){
				subjectVariables.add(s);
			}
			List<String> aliasedTriple = new ArrayList<String>();
			aliasedTriple.add(s);
			aliasedTriple.add(p);
			aliasedTriple.add(o);
			aliasedTriples.add(aliasedTriple);
		}
		Map<String, BGP> subPatterns = new HashMap<String, BGP>();
		for (List<String> aliasedTriple: aliasedTriples){
			String s = aliasedTriple.get(0);
			String p = aliasedTriple.get(1);
			String o = aliasedTriple.get(2);
			if(subPatterns.get(s) == null){
				subPatterns.put(s,  new BGP());
			}
			subPatterns.get(s).addTriple(s, p, o);
			if(subjectVariables.contains(o)){
				if(subPatterns.get(o) == null){
					subPatterns.put(o,  new BGP());
				}
				subPatterns.get(o).addTriple(s, p, o);
			}
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
		this.redisOpStack.push(new Distinct(this.redisOpStack.peek()));
	}
	
	public void visit(OpDistinct arg0) 
	{
		this.redisOpStack.push(new Distinct(this.redisOpStack.peek()));
	}

	public void visit(OpFilter arg0)
	{
		List<Expr> nonMapExprs = new ArrayList<Expr>();
		for(Expr e: arg0.getExprs()){
			Map<Node, String> nodeAliases = new HashMap<Node, String>();
			ExpressionOptimizer preProc = new ExpressionOptimizer();
			RedisExpressionVisitor rev = new RedisExpressionVisitor(ts);
			e = preProc.optimized(e);
			System.out.println("optimized expr: " + e);
			e.visit(rev);

				
			if(!redisOpStack.peek().tryAddFilter(rev)){
				nonMapExprs.add(e);
			}
		}
		for(Expr e: nonMapExprs){
			redisOpStack.push(new ReducePhaseFilter(redisOpStack.peek(), e));
			
		}
	}

	public void visit(OpJoin arg0) 
	{
		RedisOP rhs = redisOpStack.pop();
		RedisOP lhs = redisOpStack.pop();
		RedisOP joinedLHS = lhs.tryConvertToJoin(rhs, false);
		if(joinedLHS != null){
			redisOpStack.push(joinedLHS);
		} else {
			RedisJoinOP lj;
			if(RedisJoinOP.canJoinInMapPhase(lhs, rhs)){
				throw new UnsupportedOperationException();
			} else {
				lj = new ReducePhaseJoin(lhs, rhs);
			}
			redisOpStack.push(lj);
		}
	}
	
	public void visit(OpLeftJoin arg0) 
	{
		RedisOP rhs = redisOpStack.pop();
		RedisOP lhs = redisOpStack.pop();
		
		RedisOP joinedLHS = lhs.tryConvertToJoin(rhs, true);
		if(joinedLHS != null){
			redisOpStack.push(joinedLHS);
		} else {
		
			RedisJoinOP lj;
			if(RedisJoinOP.canJoinInMapPhase(lhs, rhs)){
				lj = new MapPhaseLeftJoin(lhs, rhs);
			} else {
				lj = new ReducePhaseLeftJoin(lhs, rhs);
			}
			redisOpStack.push(lj);

		}
		
		if (arg0.getExprs() != null) 
		{
			this.visit((OpFilter)OpFilter.filter(arg0.getExprs(), null));
		}
		

	}
	
	public void visit(OpUnion arg0) 
	{
		RedisOP rhs = redisOpStack.pop();
		RedisOP lhs = redisOpStack.pop();
		MapPhaseUnion result = new MapPhaseUnion(lhs, rhs);
		redisOpStack.push(result);
	}
	
	public void visit(OpOrder arg0) 
	{
		for(SortCondition c: arg0.getConditions()) 
		{
			if(redisOpStack.peek().completeAfterMapPhase()){
				RedisExpressionVisitor rev = new RedisExpressionVisitor(ts);
				c.getExpression().visit(rev);
				redisOpStack.push(new MapPhaseOrder(redisOpStack.peek(), rev));
			} else {
				redisOpStack.push(new ReducePhaseOrder(redisOpStack.peek(), c.expression));
			}
		}
	}
	
	public void visit(OpGroup arg0) 
	{
		throw new UnsupportedOperationException();
	}
	
	public void visit(OpExtend arg0) 
	{
		throw new UnsupportedOperationException();
//		System.out.println(">>>> extend  = " + arg0 );
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
		this.redisOpStack.push(new ReducePhaseSlice(this.redisOpStack.peek(), arg0.getStart(), arg0.getLength()));
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

	public void optimize() {
		// TODO Auto-generated method stub
		
	}
}