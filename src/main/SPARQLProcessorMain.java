package main;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import translate.redis.QueryResult;
import translate.sparql.SPARQLVisitor;
import translate.sparql.SPARQLRedisVisitor;

import com.beust.jcommander.JCommander;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.util.FileManager;

import main.Options;
import main.ShardedRedisTripleStoreV1;
import main.server.SPARQLEndpoint;

import org.json.*;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.util.Hashing;
import redis.clients.util.SafeEncoder;
import redis.clients.util.Sharded;

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
	    
	    //ShardedRedisTripleStore ts = new ShardedRedisTripleStoreV1(options, redisCmd, aliasShards, tripleShards);
	    ShardedRedisTripleStore ts = new ShardedRedisTripleStoreV2(options, redisCmd, aliasShards, tripleShards);
	    
	    
	    if(options.populate != null){
	    	ts.flushdb();
	    	System.out.println("Loading triple data from " + options.populate);
	    	long startTime = System.currentTimeMillis();
	    	ts.loadFromFile(options.populate);
	    	System.out.println("Finished loading data in " + (double)(System.currentTimeMillis() - startTime)/1000 + " secs");
	    }
	    
		String query1 = 
				"PREFIX ex: <http://www.example.com/> " +
				"SELECT ?name  " +
				"WHERE { " +
				" ?x ex:name ?name ." +
				"} ";
		
	
		
		String query2 = 
				"PREFIX ex: <http://www.example.com/> " +
				"SELECT ?name ?age " +
				"WHERE { " +
				" ?x ex:name ?name ." +
				" ?x ex:age ?age ." +
				"} ";
		
		
		
		String query3 = 
				"PREFIX ex: <http://www.example.com/> " +
				"SELECT ?name ?age ?phone " +
				"WHERE { " +
				" ?x ex:name ?name ." +
				" ?x ex:age ?age ." +
				" ?x ex:phone ?phone ." +
				"} ";
		
		String query4 = 
				"PREFIX ex: <http://www.example.com/> " +
				"SELECT ?name ?y " +
				"WHERE { " +
				" ?x ex:name ?name ." +
				" ?x ex:knows ?y ." +
				"} ";
		
		String query5 = 
				"PREFIX ex: <http://www.example.com/> " +
				"SELECT ?name ?age ?y ?friendsName " +
				"WHERE { " +
				" ?x ex:name ?name ." +
				" ?x ex:age ?age ." +
				" ?x ex:knows ?y ." +
				" ?y ex:name ?friendsName ." +
				"} ";
		
		String bsbm00 = 
				"PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> " +
				"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" +
				"SELECT ?product ?label $value " +
				//"SELECT ?label " +
				//"SELECT * " +
				"WHERE { " +
				"?product rdfs:label ?label ." +
				"?product bsbm:productPropertyNumeric1 ?value1 . " +
				//"FILTER (?value1 > 100) " +
				//"?product a bsbm-inst:ProductType10 ." +
				"} " ;
		
		String bsbm0 = 
				"PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> " +
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
				"SELECT ?product ?label ?type " +
				"WHERE { " +
				"?product rdfs:label ?label ." +
				//"?product a bsbm-inst:ProductType10 ." +
				"?product a ?type ." +
				"} " ;
		
		String bsbm1 = 
				"PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> " +
				"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				"" +
				"SELECT DISTINCT ?product ?label ?value1 " +
				"WHERE { " +
				    "?product rdfs:label ?label . " +
				    "?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType66> . " +
				    "?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature3> . " +
				    "?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature1967> . " +
				    "?product bsbm:productPropertyNumeric1 ?value1 . " +
					"FILTER (?value1 > 136) " +
					"} " +
				"ORDER BY ?label " +
				"LIMIT 10 ";
		
		String bsbm2 = "" +
				"PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> " +
				"	PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
				"	PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
				"	PREFIX dc: <http://purl.org/dc/elements/1.1/> " +

				"	SELECT ?label ?comment ?producer ?productFeature ?propertyTextual1 ?propertyTextual2 ?propertyTextual3  " +
				"	 ?propertyNumeric1 ?propertyNumeric2 ?propertyTextual4 ?propertyTextual5 ?propertyNumeric4  " +
				"	WHERE { " +
				"	    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> rdfs:label ?label . " +
				"	    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> rdfs:comment ?comment . " +
				"	    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> bsbm:producer ?p . " +
				"	    ?p rdfs:label ?producer . " +
				"	    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> dc:publisher ?p .  " +
				"	    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> bsbm:productFeature ?f . " +
				"	    ?f rdfs:label ?productFeature . " +
				"	    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> bsbm:productPropertyTextual1 ?propertyTextual1 . " +
				"	    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> bsbm:productPropertyTextual2 ?propertyTextual2 . " +
				"	    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> bsbm:productPropertyTextual3 ?propertyTextual3 . " +
				"	    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> bsbm:productPropertyNumeric1 ?propertyNumeric1 . " +
				"	    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> bsbm:productPropertyNumeric2 ?propertyNumeric2 . " +
				"	    OPTIONAL { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> bsbm:productPropertyTextual4 ?propertyTextual4 } " +
				"	    OPTIONAL { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> bsbm:productPropertyTextual5 ?propertyTextual5 } " +
				"	    OPTIONAL { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product72> bsbm:productPropertyNumeric4 ?propertyNumeric4 } " +
				"	} " +

				"";
		
		String bsbm3 = "" +
				"	PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> " +
				"	PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
				"	PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
				"	PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +

				"	SELECT ?product ?label ?p1 ?p3" +
				"	WHERE { " +
				"	    ?product rdfs:label ?label . " +
				"	    ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType87> . " +
				"		?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature541> . " +
				"		?product bsbm:productPropertyNumeric1 ?p1 . " +
				"		FILTER ( ?p1 > 156 )  " +
				"		?product bsbm:productPropertyNumeric3 ?p3 . " +
				"		FILTER (?p3 < 152 ) " +
				"	    OPTIONAL {  " +
				"	        ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature553> . " +
				"	        ?product rdfs:label ?testVar } " +
				"	    FILTER (!bound(?testVar))  " +
				"	} " +
				"	ORDER BY ?label " +
				"	LIMIT 10 " +
				"";
		
		String bsbm4 = "" + 
				"	PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> " +
				"	PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
				"	PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
				"	PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +

				"	SELECT DISTINCT ?product ?label ?propertyTextual " +
				"	WHERE { " +
				"	    {  " +
				"	       ?product rdfs:label ?label . " +
				"	       ?product rdf:type <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType138> . " +
//				"	       ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature4305> . " +
//				"		   ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature1427> . " +
				"	       ?product bsbm:productPropertyTextual1 ?propertyTextual . " +
				"		   ?product bsbm:productPropertyNumeric1 ?p1 . " +
				"		   FILTER ( ?p1 > 457 ) " +
				"	    } UNION { " +
				"	       ?product rdfs:label ?label . " +
				"	       ?product rdf:type <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType138> . " +
//				"	       ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature4305> . " +
//				"		   ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature1444> . " +
				"	       ?product bsbm:productPropertyTextual1 ?propertyTextual . " +
				"		   ?product bsbm:productPropertyNumeric2 ?p2 . " +
				"		   FILTER ( ?p2> 488 )  " +
				"	    }  " +
				"	} " +
				"	ORDER BY ?label " +
				"	OFFSET 5 " +
				"	LIMIT 10 " +
				"";
		
		String bsbm5 = "" +
			"	PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
			"	PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
			"	PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +

			"	SELECT DISTINCT ?product ?productLabel " +
			"	WHERE {  " +
			"		?product rdfs:label ?productLabel . " +
		    "		FILTER (<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer31/Product1390> != ?product) " +
		    "		<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer31/Product1390> bsbm:productFeature ?prodFeature . " +
		    "		?product bsbm:productFeature ?prodFeature . " +
		    "		<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer31/Product1390> bsbm:productPropertyNumeric1 ?origProperty1 . " +
			"		?product bsbm:productPropertyNumeric1 ?simProperty1 . " +
			"		FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 - 120)) " +
			"		<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer31/Product1390> bsbm:productPropertyNumeric2 ?origProperty2 . " +
			"		?product bsbm:productPropertyNumeric2 ?simProperty2 . " +
			"		FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 > (?origProperty2 - 170)) " +
			"	} " +
			"	ORDER BY ?productLabel " +
			"	LIMIT 5 " +
			"";
		
		String bsbm5b = "" +
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
			"	PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
			"	PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
	
			"	SELECT DISTINCT ?product ?productLabel " +
			"	WHERE {  " +
			"		?product rdfs:label ?productLabel . " +
			"	    FILTER (<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer41/Product1937> != ?product) " +
			"		<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer41/Product1937> bsbm:productFeature ?prodFeature . " +
			"		?product bsbm:productFeature ?prodFeature . " +
			"		<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer41/Product1937> bsbm:productPropertyNumeric1 ?origProperty1 . " +
			"		?product bsbm:productPropertyNumeric1 ?simProperty1 . " +
			"		FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 - 120)) " +
			"		<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer41/Product1937> bsbm:productPropertyNumeric2 ?origProperty2 . " +
			"		?product bsbm:productPropertyNumeric2 ?simProperty2 . " +
			"		FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 > (?origProperty2 - 170)) " +
			"	} " +
			"	ORDER BY ?productLabel " +
			"	LIMIT 5 " +
			"";
		
		// BSBM doesn't currently use query6, which is the regex query.  Thank god.
		
		String bsbm7 = "" +
				"	PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
				"	PREFIX rev: <http://purl.org/stuff/rev#> " +
				"	PREFIX foaf: <http://xmlns.com/foaf/0.1/> " +
				"	PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
				"	PREFIX dc: <http://purl.org/dc/elements/1.1/> " +

				"	SELECT ?productLabel ?offer ?price ?vendor ?vendorTitle ?vendorCountry ?date ?review ?revTitle  " +
				"	       ?reviewer ?revName ?rating1 ?rating2 " +
				"	WHERE {  " +
				"		<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer22/Product1001> rdfs:label ?productLabel . " +
				"	    OPTIONAL { " +
				"	        ?offer bsbm:product <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer22/Product1001> . " +
				"			?offer bsbm:price ?price . " +
				"			?offer bsbm:vendor ?vendor . " +
				"			?vendor rdfs:label ?vendorTitle . " +
				"	        ?vendor bsbm:country ?vendorCountry . " + //<http://downlode.org/rdf/iso-3166/countries#DE> . " +
				"	        ?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#US> . " +
				"	        ?offer dc:publisher ?vendor .  " +
				"	        ?offer bsbm:validTo ?date . " +
				"	        FILTER (?date > \"2008-06-20T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ) " +
				"	    } " +
				"	    OPTIONAL { " +
				"		?review bsbm:reviewFor <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer22/Product1001> . " +
				"		?review rev:reviewer ?reviewer . " +
				"		?reviewer foaf:name ?revName . " +
				"		?review dc:title ?revTitle . " +
				"	    OPTIONAL { ?review bsbm:rating1 ?rating1 . } " +
				"	    OPTIONAL { ?review bsbm:rating2 ?rating2 . }  " +
				"	    } " +
				"	} LIMIT 10 " +
				"";
		
		String bsbm8 = "" + 
				"	PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
				"	PREFIX dc: <http://purl.org/dc/elements/1.1/> " +
				"	PREFIX rev: <http://purl.org/stuff/rev#> " +
				"	PREFIX foaf: <http://xmlns.com/foaf/0.1/> " +
	
				"	SELECT ?title ?text ?reviewDate ?reviewer ?reviewerName ?rating1 ?rating2 ?rating3 ?rating4  " +
				"	WHERE {  " +
				"		?review bsbm:reviewFor <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer21/Product978> . " +
				"		?review dc:title ?title . " +
				"		?review rev:text ?text . " +
				"		FILTER langMatches( lang(?text), \"EN\" )  " +
				"		?review bsbm:reviewDate ?reviewDate . " +
				"		?review rev:reviewer ?reviewer . " +
				"		?reviewer foaf:name ?reviewerName . " +
				"		OPTIONAL { ?review bsbm:rating1 ?rating1 . } " +
				"		OPTIONAL { ?review bsbm:rating2 ?rating2 . } " +
				"		OPTIONAL { ?review bsbm:rating3 ?rating3 . } " +
				"		OPTIONAL { ?review bsbm:rating4 ?rating4 . } " +
				"	} " +
				"	ORDER BY DESC(?reviewDate) " +
				"	LIMIT 20 " +
				"";
		
		String bsbm9 = "" +
				"	PREFIX rev: <http://purl.org/stuff/rev#> " +

				"	DESCRIBE ?x " +
				"	WHERE { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Review5659> rev:reviewer ?x } " +
				"";
		
		String bsbm10 = "" +
				"	PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
				"	PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>  " +
				"	PREFIX dc: <http://purl.org/dc/elements/1.1/> " +

				"	SELECT DISTINCT ?offer ?price " +
				"	WHERE { " +
				"		?offer bsbm:product <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer9/Product396> . " +
				"		?offer bsbm:vendor ?vendor . " +
				"	    ?offer dc:publisher ?vendor . " +
				"		?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#US> . " +
				"		?offer bsbm:deliveryDays ?deliveryDays . " +
				"		FILTER (?deliveryDays <= 3) " +
				"		?offer bsbm:price ?price . " +
				"	    ?offer bsbm:validTo ?date . " +
				"	    FILTER (?date > \"2008-06-20T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ) " +
				"	} " +
				"	ORDER BY xsd:double(str(?price)) " +
				"	LIMIT 10 " +
				"";
		
		String bsbm11 = "" +
				"	SELECT ?property ?hasValue ?isValueOf " +
				"		WHERE { " +
				"		  { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor12/Offer21250> ?property ?hasValue } " +
				"		  UNION " +
				"		  { ?isValueOf ?property <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor12/Offer21250> } " +
				"		} " +
				"";
		
		String bsbm12 = "" +
				"	PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
				"	PREFIX rev: <http://purl.org/stuff/rev#> " +
				"	PREFIX foaf: <http://xmlns.com/foaf/0.1/> " +
				"	PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
				"	PREFIX bsbm-export: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/export/> " +
				"	PREFIX dc: <http://purl.org/dc/elements/1.1/> " +

				"	CONSTRUCT {  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm-export:product ?productURI . " +
				"	             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm-export:productlabel ?productlabel . " +
				"	             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm-export:vendor ?vendorname . " +
				"	             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm-export:vendorhomepage ?vendorhomepage .  " +
				"	             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm-export:offerURL ?offerURL . " +
				"	             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm-export:price ?price . " +
				"	             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm-export:deliveryDays ?deliveryDays . " +
				"	             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm-export:validuntil ?validTo }  " +
				"	WHERE { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm:product ?productURI . " +
				"	        ?productURI rdfs:label ?productlabel . " +
				"	        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm:vendor ?vendorURI . " +
				"	        ?vendorURI rdfs:label ?vendorname . " +
				"	        ?vendorURI foaf:homepage ?vendorhomepage . " +
				"	        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm:offerWebpage ?offerURL . " +
				"	        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm:price ?price . " +
				"	        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm:deliveryDays ?deliveryDays . " +
				"	        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm:validTo ?validTo } " +
				"";
		
		
		if(options.listen){
			try {
				//System.out.println(ts.dbInfo());
				SPARQLEndpoint.listen(options.listenPort, ts);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//		} else {
//			QueryResult result = RedisQueryExecution.execute(bsbm9, ts);
//
//			System.out.println(result.asTable());
		}
		
		ts.killThreads();

	}

}
