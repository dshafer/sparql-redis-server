package main;

import translate.redis.QueryResult;
import translate.sparql.SPARQLRedisVisitor;

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
		Query q = QueryFactory.create(qs);
		switch(q.getQueryType()){
		case Query.QueryTypeSelect:
			return select(q, ts);
		case Query.QueryTypeDescribe:
			return describe(q, ts);
		case Query.QueryTypeConstruct:
			return construct(q, ts);
		}
		throw new UnsupportedOperationException();
	}

	static private QueryResult construct(Query q, ShardedRedisTripleStore ts) {
		// TODO Auto-generated method stub
		return null;
	}

	static private QueryResult describe(Query q, ShardedRedisTripleStore ts) {
		SPARQLRedisVisitor v = getVisitor(q, ts);
		v.visit(new OpDistinct(null));
		QueryResult result = ts.execute(v);
		result.unalias(ts);
		
		if(result.rows.size()>1){
			throw new UnsupportedOperationException("Distinct query returned too many subjects");
		} else if (result.rows.size() == 0){
			throw new UnsupportedOperationException("No subjects returned");
		}
		Node s = result.rows.get(0).get(0);
		if(!s.isURI()){
			throw new UnsupportedOperationException("subject is not a URI");
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
		Op op = Algebra.compile(q);
		System.out.println("This is the Abstract Syntax Tree: ");
		System.out.println(op.toString());
		
		System.out.println("Walking the tree: ");
		SPARQLRedisVisitor v = new SPARQLRedisVisitor(ts);
		//SPARQLVisitor v = new SPARQLVisitor();
		OpWalker.walk(op, v);
		System.out.println("Translated query :\n" + v.toString());
		System.out.println("Map script is: \n" + ts.luaScriptBoilerplate() + v.luaMapScript());
		
		return v;
	}
	
	

}
