package translate.redis;

import java.util.Set;
import java.util.Stack;

import com.hp.hpl.jena.sparql.expr.Expr;

public abstract class RedisJoinOP implements RedisOP {

	RedisOP lhs;
	RedisOP rhs;
	
	static public boolean canJoinInMapPhase(RedisOP lhs, RedisOP rhs){
		return false;
	}
	
	public RedisJoinOP(RedisOP _lhs, RedisOP _rhs){
		lhs = _lhs;
		rhs = _rhs;
	}
	
	@Override
	public String mapLuaScript() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		// TODO Auto-generated method stub
		return null;
	}

	public Boolean tryAddFilter(RedisExpressionVisitor rev) {
		Boolean result = lhs.tryAddFilter(rev);
		result |= rhs.tryAddFilter(rev);
		return result;
	}

	@Override
	public RedisOP tryConvertToJoin(RedisOP op, Boolean left) {
		RedisOP attempt = lhs.tryConvertToJoin(op, left);
		if(attempt != null){
			this.lhs = attempt;
			return this;
		}
		attempt = rhs.tryConvertToJoin(op, left);
		if(attempt != null){
			this.rhs = attempt;
			return this;
		}
		return null;
	}
	

	@Override
	public Set<String> getJoinVariables() {
		Set<String> result = lhs.getJoinVariables();
		result.addAll(rhs.getJoinVariables());
		return result;
	}
}
