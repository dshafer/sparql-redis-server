package com.drewshafer.sparql;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.drewshafer.sparql.backend.redis.ShardedRedisTripleStore;
import com.drewshafer.sparql.backend.redis.ShardedRedisTripleStoreV2;

import com.drewshafer.sparql.server.SPARQLEndpoint;
import org.json.*;
import redis.clients.jedis.JedisShardInfo;

public class SPARQLProcessorMain {

	
	private static String readFile(String path) throws IOException {
	  FileInputStream stream = new FileInputStream(new File(path));
	  try {
	    FileChannel fc = stream.getChannel();
	    MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
	    /* Instead of using default, pass in a decoder. */
	    return Charset.defaultCharset().decode(bb).toString();
	  }
	  finally {
	    stream.close();
	  }
	}
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		Options options = new Options();
	    JCommander comm =  new JCommander(options, args);
	    if(options.getHelp){
	    	comm.usage();
	    	System.exit(0);
	    }
	    
	    // set up redis shards
	    String redisJson = readFile(options.redisClusterConfig);
	    JSONObject jO = new JSONObject(redisJson);
	    String redisCmd = jO.getString("redisCmd");
	    List<JedisShardInfo> aliasShards = new ArrayList<JedisShardInfo>();
	    List<JedisShardInfo> tripleShards = new ArrayList<JedisShardInfo>();
	    if(jO.has("nodes")){
		    int aliasBasePort = 50720;
		    int tripleBasePort = 50820;
		    if(jO.has("aliasBasePort")){
		    	aliasBasePort = jO.getInt("aliasBasePort");
		    }
		    if(jO.has("tripleBasePort")){
		    	tripleBasePort = jO.getInt("tripleBasePort");
		    }
		    JSONObject nodes = jO.getJSONObject("nodes");
		    
		    for (String hostName : JSONObject.getNames(nodes)){
		    	JSONArray hA = nodes.getJSONArray(hostName);
		    	int numHostAliasDbs = hA.getInt(0);
		    	int numHostTripleDbs = hA.getInt(1);
		    	for(int x = 0; x < numHostAliasDbs; x++){
		    		aliasShards.add(new JedisShardInfo(hostName, aliasBasePort + x));
		    	}	    	
		    	for(int x = 0; x < numHostTripleDbs; x++){
		    		tripleShards.add(new JedisShardInfo(hostName, tripleBasePort + x));
		    	}
		    }
		    for(JedisShardInfo ji : aliasShards){
		    	ji.setTimeout(9999 * 1000);
		    }
		    for(JedisShardInfo ji : tripleShards){
		    	ji.setTimeout(9999 * 1000);
		    }
	    } else {
	    
		    JSONArray jAliasShards = jO.getJSONArray("aliasDb");
		    for(int x=0; x < jAliasShards.length(); x++){
		    	JSONObject jAlias = jAliasShards.getJSONObject(x);
		    	JedisShardInfo aliasShard = new JedisShardInfo(jAlias.getString("host"), jAlias.getInt("port"));
			    aliasShard.setTimeout(99999 * 1000);
			    aliasShards.add(aliasShard);
		    }

		    JSONArray jTripleShards = jO.getJSONArray("tripleDb");
		    for(int x = 0; x < jTripleShards.length(); x++){
		    	JSONObject jTripleShard = jTripleShards.getJSONObject(x);
		    	String serverHost = jTripleShard.getString("host");
		    	int serverPort = jTripleShard.getInt("port");
		    	
	    		JedisShardInfo shard = new JedisShardInfo(serverHost, serverPort);
	    		shard.setTimeout(99999 * 1000);
		    	tripleShards.add(shard);
		    }
	    }
	    
	    ShardedRedisTripleStore ts = new ShardedRedisTripleStoreV2(options, redisCmd, aliasShards, tripleShards);
	    
	    
	    if(options.populate != null){
	    	System.out.println("Loading triple data from " + options.populate);
	    	long startTime = System.currentTimeMillis();
	    	ts.loadFromFile(options.populate);
	    	System.out.println("Finished loading data in " + (double)(System.currentTimeMillis() - startTime)/1000 + " secs");
	    }
	    
	
		if(options.listen){
			try {
				SPARQLEndpoint.listen(options.listenPort, ts);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		ts.killThreads();

	}

}
