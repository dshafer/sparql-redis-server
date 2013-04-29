package translate.redis;

import java.util.Set;
import java.util.Stack;

import com.hp.hpl.jena.sparql.expr.Expr;

import main.ShardedRedisTripleStore;

public interface RedisOP {
	public String mapLuaScript();
	public QueryResult reduce(Stack<QueryResult> patternStack);
	public Boolean completeAfterMapPhase();
	public String toString(String indent);
	public Boolean tryAddFilter(RedisExpressionVisitor rev);
	public RedisOP tryConvertToJoin(RedisOP op, Boolean left);
	public Set<String> getJoinVariables();
}
