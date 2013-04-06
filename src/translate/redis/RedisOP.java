package translate.redis;

import java.util.Stack;

import main.ShardedRedisTripleStore;

public interface RedisOP {
	public String mapLuaScript();
	public QueryResult reduce(Stack<QueryResult> patternStack);
	public Boolean completeAfterMapPhase();
}
