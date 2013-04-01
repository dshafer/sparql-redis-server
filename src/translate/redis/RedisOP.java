package translate.redis;

import main.ShardedRedisTripleStore;
import main.DataTypes.GraphResult;

public interface RedisOP {
	public GraphResult execute(ShardedRedisTripleStore ts, String graphPatternKey, String scratchKey);
}
