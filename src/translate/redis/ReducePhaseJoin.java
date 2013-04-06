package translate.redis;

import java.util.Stack;

import main.ShardedRedisTripleStore;

public class ReducePhaseJoin implements RedisOP{

	public ReducePhaseJoin(RedisOP left, RedisOP right) {
		// TODO Auto-generated constructor stub
	}

	@Override
	public String mapLuaScript() {
		throw new UnsupportedOperationException();
	}

	@Override
	public QueryResult reduce(Stack<QueryResult> input) {
		return null;
	}

	@Override
	public Boolean completeAfterMapPhase() {
		// TODO Auto-generated method stub
		return false;
	}
	
}
