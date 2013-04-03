package translate.redis;

import main.ShardedRedisTripleStore;
import main.DataTypes.GraphResult;

public interface RedisOP {
	public String execute(ShardedRedisTripleStore ts, String keyspace, String graphPatternKey);
}
