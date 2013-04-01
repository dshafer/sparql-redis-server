/*
 * (C) Copyright 2011 - Juan F. Sequeda and Daniel P. Miranker
 * Permission to use this code is only granted by separate license 
 */
package translate.sql;

import java.io.PrintStream;
import java.util.Map;
import java.util.Set;

import com.hp.hpl.jena.sparql.core.Var;

public interface SqlOP 
{

	public void projectVariable(String name);
	
	public void projectJoinVariable(String name);
	
	public String resolveVariable(String name);
	
	public String resolveJoinVariable(String name);
	
	public Set<String> getVariables();
	
	public void addFilter(SqlExpr expr);
	
	public void orderBy(SqlExpr expr, boolean up);
	
	public void slice(long start, long length) throws Exception;
	
	public void distinct();
	
	public String getName();
	
	public void print(PrintStream out, int indent);

	public void addAggregator(SqlExpr expr);

	public void addExtend(String internalVar, SqlExpr externalExpression);
	
	public void addGroupBy(Var var);
	
	public Map<String, SqlExpr> getAggregatorMap();

	public void addHaving(SqlExpr expr);
}