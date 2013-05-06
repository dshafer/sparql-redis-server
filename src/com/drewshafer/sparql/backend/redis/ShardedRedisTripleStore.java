package com.drewshafer.sparql.backend.redis;

import java.util.Set;

import com.hp.hpl.jena.graph.Node;

import java.util.Map;

public interface ShardedRedisTripleStore {
	void flushdb();
	void loadFromFile(String filename);
	String dbInfo();
	QueryResult execute(SPARQLRedisVisitor v);
	String getAliasOrLiteralValue(Node asNode);
	Map<String, Node> getNodesFromAliases(Set<String> aliases);
	String getAlias(Node sn);
	String luaScriptBoilerplate();
	void killThreads();
}