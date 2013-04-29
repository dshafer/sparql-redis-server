package translate.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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

public abstract class RedisFilterOP implements RedisOP{
	
	List<Expr> expressions;
	RedisOP parent;
	public RedisFilterOP(RedisOP _parent){
		expressions = new ArrayList<Expr>();
		this.parent = _parent;
	}
	
	@Override
	public String mapLuaScript() {
		return parent.mapLuaScript();

	}
	
	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		return parent.reduce(patternStack);
	}

	public void addFilter(Expr e){
		expressions.add(e);
	}

	@Override
	public Set<String> getJoinVariables() {
		return parent.getJoinVariables();
	}
	
}
