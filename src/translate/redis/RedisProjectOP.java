package translate.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.hp.hpl.jena.sparql.expr.Expr;

public abstract class RedisProjectOP implements RedisOP{
	List<String> projectedVariables;
	protected RedisOP parent;
	
	public RedisProjectOP(RedisOP _parent){
		projectedVariables = new ArrayList<String>();
		this.parent = _parent;
		
	}
	
	public void projectVariable(String name) {
		projectedVariables.add('?' + name);
		
	}
	
	@Override
	public Boolean tryAddFilter(RedisExpressionVisitor rev) {
		return parent.tryAddFilter(rev);
	}

	@Override
	public RedisOP tryConvertToJoin(RedisOP op, Boolean left) {
		RedisOP attempt = parent.tryConvertToJoin(op, left);
		if(attempt != null){
			parent = attempt;
			return this;
		}
		return null;
	}

	@Override
	public Set<String> getJoinVariables() {
		return parent.getJoinVariables();
	}
}
