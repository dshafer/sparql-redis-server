package com.drewshafer.sparql;

import com.drewshafer.sparql.backend.redis.SPARQLRedisVisitor;
import com.drewshafer.sparql.backend.redis.ShardedRedisTripleStore;
import com.drewshafer.sparql.backend.redis.QueryResult;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;

public class RedisQueryExecution {
	
	static public QueryResult execute(String qs, ShardedRedisTripleStore ts){
		long startTime = System.currentTimeMillis();
		Query q = QueryFactory.create(qs);
		
		
		
		QueryResult r = null;
		switch(q.getQueryType()){
			case Query.QueryTypeSelect:
				r = select(q, ts);
				break;
			case Query.QueryTypeDescribe:
				r = describe(q, ts);
				break;
			case Query.QueryTypeConstruct:
				r = construct(q, ts);
				break;
		}
		if(r == null){
			throw new UnsupportedOperationException("unknown query type");
		}
		System.out.println("Total query execution time: " + (double)(System.currentTimeMillis() - startTime)/1000 + " seconds");
		return r;
	}

	static private QueryResult construct(Query q, ShardedRedisTripleStore ts) {
		return select(q, ts);
	}

	static private QueryResult describe(Query q, ShardedRedisTripleStore ts) {
		long startTime = System.currentTimeMillis();
		SPARQLRedisVisitor v = getVisitor(q, ts);
		v.visit(new OpDistinct(null));
		QueryResult result = ts.execute(v);
		result.unalias(ts);
		
		System.out.println("Describe (stage 1) : " + (double)(System.currentTimeMillis() - startTime)/1000 + " seconds");
		
		if(result.rows.size()>1){
			System.out.println("Warning: describe query produced multiple subjects.  Only reporting on first subject.");
			
		} else if (result.rows.size() == 0){
			return result;
		}
		Node s = result.rows.get(0).get(0);
		if(!s.isURI()){
			throw new UnsupportedOperationException("subject is not a URI.");
		}
		String describeQuery = "" +
				"	SELECT ?p ?o " +
				"	WHERE { <" + s.getURI() + "> ?p ?o } " +
				"";
		q = QueryFactory.create(describeQuery);
		return select(q, ts);

	}

	static private QueryResult select(Query q, ShardedRedisTripleStore ts) {
		SPARQLRedisVisitor v = getVisitor(q, ts);
		QueryResult result = ts.execute(v);
		result.unalias(ts);
		return result;
	}
	
	static private SPARQLRedisVisitor getVisitor(Query q, ShardedRedisTripleStore ts){
		long startTime = System.currentTimeMillis();
		Op op = Algebra.compile(q);
		//System.out.println("This is the Abstract Syntax Tree: ");
		//System.out.println(op.toString());
		
		//System.out.println("Walking the tree: ");
		SPARQLRedisVisitor v = new SPARQLRedisVisitor(ts);
		//SPARQLVisitor v = new SPARQLVisitor();
		OpWalker.walk(op, v);
		System.out.println("Translated query :\n" + v.toString());
//		System.out.println("Map script is: \n" + ts.luaScriptBoilerplate() + v.luaMapScript());
		System.out.println("Query parsing/optimizing: " + (double)(System.currentTimeMillis() - startTime)/1000 + " seconds");
		return v;
	}
	
	

}
