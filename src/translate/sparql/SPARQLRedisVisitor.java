/*
 * (C) Copyright 2011 - Juan F. Sequeda and Daniel P. Miranker
 * Permission to use this code is only granted by separate license 
 */
package translate.sparql;

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
import translate.redis.Distinct;
import translate.redis.Filter;
import translate.redis.Project;
import translate.redis.RedisOP;


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
	
	int tripleIndex = 0;
	int bgpIndex = 0;
	int joinIndex = 0;
	//Stack<SqlOP> sqlOpStack = new Stack<SqlOP>();
	Stack<RedisOP> redisOpStack = new Stack<RedisOP>();
	String baseURI = "";
	int maxNumOfKeys = 0;
	
	public SPARQLRedisVisitor() 
	{
	
	}
	
	private String _execute(ShardedRedisTripleStore ts, String keyspace, String graphPatternKey){
		RedisOP op = redisOpStack.pop();
		if (!redisOpStack.isEmpty()){
			graphPatternKey = _execute(ts, keyspace, graphPatternKey);
		}
		return op.execute(ts, keyspace, graphPatternKey);
	}
	
	public String execute(ShardedRedisTripleStore ts){
		String keyspace = "redisSparql:";
		String graphPatternKey = keyspace + "working:graphResult";
		// make sure the initial graph pattern input is empty
		for(Jedis j: ts.shards){
			j.del(graphPatternKey);
		}
		if (!redisOpStack.isEmpty()){
			return _execute(ts, keyspace, graphPatternKey);
		}
		return null;
	}
	
	public void visit(OpProject arg0) 
	{
		System.out.println(">>>> project = " + arg0.getVars());
		Project pOp = new Project();
		for (Var v : arg0.getVars()) 
		{
			pOp.projectVariable(v.getName());
		}
		this.redisOpStack.push(pOp);
	}

	
	public void visit(OpBGP bgp) 
	{
		System.out.println(">>>> bgp " + bgpIndex + " = " + bgp.getPattern() );
		BGP redisBGP = new BGP();
		for (Triple t : bgp.getPattern()) 
		{
			try 
			{
				redisBGP.addTriple(t);
				
			} 
			catch (Exception e) 
			{
				e.printStackTrace();
			}
		}
		
		this.redisOpStack.push(redisBGP);
	}
	
	public void visit(OpReduced arg0) 
	{
		System.out.println(">>>> reduced");
		this.redisOpStack.push(new Distinct());
	}
	
	public void visit(OpDistinct arg0) 
	{
		System.out.println(">>>> distinct");
		this.redisOpStack.push(new Distinct());
	}

	public void visit(OpFilter arg0) 
	{
		System.out.println(">>>> filter = " + arg0.getExprs());
		//Map<String, SqlExpr> aggregatorMap = sqlOpStack.peek().getAggregatorMap();
		Filter f = new Filter();
		for(Expr e: arg0.getExprs()) 
		{
			System.out.println(">>>>>> expr = " + e);
			f.addFilter(e);
		}
		this.redisOpStack.push(f);
	}

	public void visit(OpJoin arg0) 
	{
		System.out.println(">>>> join " + joinIndex + " = " + arg0 );
//		SqlOP rhs = sqlOpStack.pop();
//		SqlOP lhs = sqlOpStack.pop();
//		sqlOpStack.push(new JOIN(++joinIndex, lhs, rhs));
	}
	
	public void visit(OpLeftJoin arg0) 
	{
		System.out.println(">>>> leftjoin " + joinIndex + " = " + arg0 );
		// Filters passed to the left join constructor are included
		// in the 'ON' clause of the left-join operator.
		// Filters added using the Query.addFilter method are added 
		// to the WHERE clause of the left join query.
		//List<SqlExpr> filters = new LinkedList<SqlExpr>();
		
		if (arg0.getExprs() != null) 
		{
			this.visit((OpFilter)OpFilter.filter(arg0.getExprs(), null));
			
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