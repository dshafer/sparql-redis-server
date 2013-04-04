package main;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

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
import main.ShardedRedisTripleStore;
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
	    new JCommander(options, args);
	    
	    // set up redis shards
	    String redisJson = readFile(options.redisConfig);
	    JSONObject jO = new JSONObject(redisJson);
	    JSONObject jAlias = jO.getJSONObject("aliasDb");
	    JedisShardInfo aliasShard = new JedisShardInfo(jAlias.getString("host"), jAlias.getInt("port"));
	    List<JedisShardInfo> tripleShards = new ArrayList<JedisShardInfo>();
	    JSONArray jTripleShards = jO.getJSONArray("tripleDb");
	    for(int x = 0; x < jTripleShards.length(); x++){
	    	JSONObject jTripleShard = jTripleShards.getJSONObject(x);
	    	String serverHost = jTripleShard.getString("host");
	    	int serverPort = jTripleShard.getInt("port");
	    	JedisShardInfo shard = new JedisShardInfo(serverHost, serverPort);
	    	tripleShards.add(shard);
	    }
	    
	    ShardedRedisTripleStore ts = new ShardedRedisTripleStore(aliasShard, tripleShards);
	    
	    if(options.populate != null){
	    	ts.flushdb();
	    	Model model = ModelFactory.createDefaultModel();
	        InputStream is = FileManager.get().open(options.populate);
	        if (is != null) {
	            model.read(is, null, "N-TRIPLE");
	        } else {
	            System.err.println("cannot read " + options.populate);;
	        }
	        StmtIterator sI = model.listStatements();
	        while (sI.hasNext()) {
	        	Statement s = sI.nextStatement();
	        	ts.insertTriple(s.asTriple());
	        }
	        sI.close();
	    	
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
				//"SELECT ?product ?label " +
				//"SELECT ?label " +
				"SELECT * " +
				"WHERE { " +
				"?product rdfs:label ?label ." +
				"?product bsbm:productPropertyNumeric1 ?value1 . " +
				"FILTER (?value1 > 100) " +
				//"?product a bsbm-inst:ProductType10 ." +
				"} " ;
		
		String bsbm0 = 
				"PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> " +
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" +
				"SELECT DISTINCT ?product ?label " +
				"WHERE { " +
				"?product rdfs:label ?label ." +
				"?product a bsbm-inst:ProductType10 ." +
				"} " ;
		
		String bsbm1 = 
				"PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> " +
				"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				"SELECT DISTINCT ?product ?label ?value" +
				"WHERE { " +
				"?product rdfs:label ?label ." +
				//"?product a bsbm-inst:ProductType10 ." +
				//"?product bsbm:productFeature bsbm-inst:ProductFeature1 ." +
				//"?product bsbm:productFeature bsbm-inst:ProductFeature2 . " +
				"?product bsbm:productPropertyNumeric1 ?value1 . " +
				"FILTER ((?value1 > 100) || (?value1 < 90)) " +
				//"FILTER (?value1 > 100) " +
				"} " ;
				//"ORDER BY ?label " +
				//"LIMIT 10";
		
		String bsbm2 = 
				"PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> " +
				"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
				"PREFIX dc: <http://purl.org/dc/elements/1.1/> " +
				"SELECT ?label ?comment ?producer ?productFeature ?propertyTextual1 ?propertyTextual2 ?propertyTextual3 ?propertyNumeric1 ?propertyNumeric2 ?propertyTextual4 ?propertyTextual5 ?propertyNumeric4  " +
				"WHERE { " +
				"FILTER(?x = <Product123>) " +
				"?x rdfs:label ?label . " +
				"?x rdfs:comment ?comment ." +
				"?x bsbm:productPropertyTextual1 ?propertyTextual1 ." +
				"?x bsbm:productPropertyTextual2 ?propertyTextual2 ." +
				"?x bsbm:productPropertyTextual3 ?propertyTextual3 ." +
				"?x bsbm:productPropertyNumeric1 ?propertyNumeric1 ." +
				"?x bsbm:productPropertyNumeric2 ?propertyNumeric2 ." +
				"?x bsbm:producer ?p . " +
				"?x bsbm:productFeature ?f . " +
				"?p rdfs:label ?producer . " +
				"?f rdfs:label ?productFeature ." +
				"OPTIONAL { ?x bsbm:productPropertyTextual4 ?propertyTextual4 }" +
				"OPTIONAL { ?x bsbm:productPropertyTextual5 ?propertyTextual5 }" +
				"OPTIONAL { ?x bsbm:productPropertyNumeric4 ?propertyNumeric4 }" +
				"}";
		
		String bsbm3 = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> " +
				"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				"SELECT ?product ?label " +
				"WHERE { " +
				"?product rdfs:label ?label . " +
				"?product a <ProductType> ." +
				"?product bsbm:productFeature <ProductFeature1> ." +
				"?product bsbm:productPropertyNumeric1 ?p1 ." +
				"FILTER ( ?p1 > 100 ) " +
				"?product bsbm:productPropertyNumeric3 ?p3 ." +
				"FILTER (?p3 < 300 ) " +
				"OPTIONAL { " +
				" ?product bsbm:productFeature <ProductFeature2> ." +
				" ?product rdfs:label ?testVar }" +
				" FILTER (!bound(?testVar)) }";
		
				
		Query q = QueryFactory.create(bsbm00);
		Op op = Algebra.compile(q);
		System.out.println("This is the Abstract Syntax Tree: ");
		System.out.println(op.toString());
		
		System.out.println("Going to walk the tree: ");
		SPARQLRedisVisitor v = new SPARQLRedisVisitor();
		//SPARQLVisitor v = new SPARQLVisitor();
		OpWalker.walk(op, v);
		v.execute(ts);
		System.out.println(v);
		

	}

}
