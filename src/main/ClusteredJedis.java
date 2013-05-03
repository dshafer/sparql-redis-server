package main;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.exceptions.JedisDataException;

public class ClusteredJedis {
	final List<JedisShardInfo> shardInfos;
	final protected ThreadLocal<List<Jedis>> shards = new ThreadLocal<List<Jedis>>(){
		@Override
		protected List<Jedis> initialValue(){
			List<Jedis> result = new ArrayList<Jedis>();
			for (JedisShardInfo shard : shardInfos){
				Jedis j = null;
				
				if(shard != null){
					j = new Jedis(shard);
					waitForRedis(j);
				}
				
				result.add(j);
			}
			return result;
		}
	};
	final int numShards;
	final String redisCmd;
	final String dataDir;
	final String redisBaseConfigFile;
	Boolean dbsStarted;
	private ExecutorService foregroundExecutor;
	private ExecutorService backgroundExecutor;
	final AtomicInteger backgroundQueueSize;
	int maxBackgroundQueueLength;
	String clusterName;
	Boolean simulate;
	
	public ClusteredJedis(String clusterName, String redisCmd, String redisBaseConfigFile, String dataDir, List<JedisShardInfo> shardInfos, int numThreads){
		this.clusterName = clusterName;
		this.shardInfos = shardInfos;
		this.numShards = shardInfos.size();
		this.redisCmd = redisCmd;
		if(!dataDir.endsWith("/")){
			this.dataDir = this.dataDir + "/";
		} else {
			this.dataDir = dataDir;
		}
		this.redisBaseConfigFile = redisBaseConfigFile;
		foregroundExecutor = Executors.newFixedThreadPool(numThreads);
		backgroundExecutor = Executors.newFixedThreadPool(numThreads);
		backgroundQueueSize = new AtomicInteger(0);
		maxBackgroundQueueLength = numThreads * 2;
	}
	
	public void killThreads(){
		foregroundExecutor.shutdownNow();
		backgroundExecutor.shutdownNow();
	}
	
	protected void waitForCluster(){
		for(Jedis db : shards.get()){
			waitForRedis(db);
		}
	}
	
	private void waitForRedis(Jedis db){
		Boolean done = false;
		while (!done){
			try {
				db.get("test");
				done = true;
			}
			catch (JedisDataException jde){
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					//pass
				}
			} catch (Exception e){
				System.err.println("Unknown Error while attempting to connect to Jedis: " + e.getMessage());
				System.exit(1);
			}
		}
	}
	
	public void flushdb(){
		for(Jedis db:shards.get()){
			if(db != null){
				db.flushDB();
			}
		}
	}
	
	// keeping this dumb and simple for now.
	protected int calcShardIdx(String key){
		byte[] bytesOfMessage;
		try {
			bytesOfMessage = key.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			return 0;
		}
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			return 0;
		}
		byte[] digest = md.digest(bytesOfMessage);
		
		// we'll just use the first 2 bytes to calculate the shard index for simplicity
		int lowOrder = digest[0] + (256 * digest[1]);
		return Math.abs(lowOrder % numShards);
	}
	
	
	private void startDb(String redisCmd, String redisConfig, String dbId, String dataDir, JedisShardInfo si){
		if (si != null){
			StringBuilder cmd = new StringBuilder();
	
			if(!si.getHost().equals("127.0.0.1")){
				cmd.append("ssh " + si.getHost() + " ");
			}
			
			cmd.append(redisCmd);
			cmd.append(" " + redisConfig);
			cmd.append(" --daemonize yes");
			cmd.append(" --port " + si.getPort());
			cmd.append(" --pidfile " + dataDir + "pid_" + dbId);
			
			if(!dataDir.isEmpty()){
				cmd.append(" --dbfilename " + dataDir + "db_" + dbId + ".rdb");
			}
			
			Runtime r = Runtime.getRuntime();
			try {
				r.exec(cmd.toString()).waitFor();
				System.out.println("Started redis-server " + dbId + " on " + si.getHost() + ":" + si.getPort());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	protected void startDatabases(){
		try {
			Runtime.getRuntime().exec("mkdir -p " + dataDir).waitFor();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		for(int x = 0; x < numShards; x++){
			startDb(redisCmd, redisBaseConfigFile, clusterName + "_" + x, dataDir, shardInfos.get(x));
		}
		waitForCluster();
	}
	
	protected void runInParallelAndWait(List<Runnable> tasks){
		final AtomicInteger taskCompleteCounter = new AtomicInteger(0);
		final class task implements Runnable {

			final Runnable r;
			public task(Runnable r){
				this.r = r;
			}
			@Override
			public void run() {
				r.run();
				taskCompleteCounter.incrementAndGet();
			}
		}
		
		int numTasks = tasks.size();
		for(Runnable r : tasks){
			foregroundExecutor.execute(new task(r));
//			(new task(r)).run();
		}
		while(taskCompleteCounter.get() < numTasks){
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				//pass
			}
		}
	}
	
	// Note - will still block if pending queue is too large
	public void runInParallelBackgrounded(List<Runnable> tasks){
		for(Runnable r : tasks){
			backgroundQueueSize.incrementAndGet();
		}
		
		while(backgroundQueueSize.get() > maxBackgroundQueueLength){
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				//pass
			}
		}
		
		final AtomicInteger taskCompleteCounter = new AtomicInteger(0);
		final class task implements Runnable {
			final Runnable r;
			public task(Runnable r){
				this.r = r;
			}
			@Override
			public void run() {
				try{
					r.run();
				} finally {
					taskCompleteCounter.incrementAndGet();
					backgroundQueueSize.decrementAndGet();
				}
			}
		}
		
		int numTasks = tasks.size();
		for(Runnable r : tasks){
			backgroundExecutor.execute(new task(r));
			//(new task(r)).run();
		}
		while(taskCompleteCounter.get() < numTasks){
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				//pass
			}
		}
	}

	public void waitForBackgroundThreads(){
		while(backgroundQueueSize.get() > 0){
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				//pass
			}
		}
	}
	public String loadScript(String script) {
		String result = null;
		for(Jedis db:shards.get()){
			if(db != null){
				result = db.scriptLoad(script);
			}
		}
		return result;
	}
	
	protected String claimUniqueKey(Jedis db){
		int keyIdx = 0;
		String result = "__META_uniqKey" + keyIdx;
		while (db.hsetnx("__META_uniqKeyLease", result, "true") == 0){
			keyIdx++;
			result = "__META_uniqKey" + keyIdx;
		}
		return result;
	}
	
	protected void releaseUniqueKey(Jedis db, String key){
		long result = db.del(key);
		db.hdel("__META_uniqKeyLease", key);
	}
	
	public String dbInfo(){
		StringBuilder result = new StringBuilder();
		
		int x = 0;
		for(Jedis j : shards.get()){
			result.append("\nnode[" + x + "]:\n");
			if(j == null){
				result.append("  not active\n");
			} else {
				result.append(j.info() + "\n");
			}
		}
		return result.toString();
	}
}
