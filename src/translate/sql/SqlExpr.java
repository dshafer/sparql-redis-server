/*
 * (C) Copyright 2011 - Juan F. Sequeda and Daniel P. Miranker
 * Permission to use this code is only granted by separate license 
 */
package translate.sql;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import com.hp.hpl.jena.sparql.expr.ExprFunction0;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.ExprFunction3;
import com.hp.hpl.jena.sparql.expr.ExprFunctionN;
import com.hp.hpl.jena.sparql.expr.ExprFunctionOp;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.ExprVisitor;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.expr.aggregate.Aggregator;


public class SqlExpr implements ExprVisitor 
{

	StringBuilder sqlExpr = new StringBuilder();
	List<String> variables = new LinkedList<String>();
	
	final Map<String,String> operatorMap = new TreeMap<String,String>();
	final Map<String, Boolean> unaryOpFix = new TreeMap<String, Boolean>();
	
	public String aggregatorInternalVariable;
	private String baseURI;
	
	public SqlExpr(String baseURI) 
	{
		this.baseURI = baseURI;
		operatorMap.put("=", "=");
		operatorMap.put("!=", "!=");
		operatorMap.put(">", ">");
		operatorMap.put("<", "<");
		operatorMap.put("<=", "<=");
		operatorMap.put(">=", ">=");
		operatorMap.put("&&", "AND");
		operatorMap.put("+", "+");
		operatorMap.put("*", "*");
		operatorMap.put("/", "/");
		operatorMap.put("-", "-");
		operatorMap.put("||", "OR");
		operatorMap.put("not", "NOT");
		operatorMap.put("bound", "IS NOT NULL");
		operatorMap.put("str", ""); // TODO HACK
		operatorMap.put("langMatches", "="); // TODO HACK
		operatorMap.put("contains", "contains"); // TODO HACK
		
		unaryOpFix.put("not", true);
		unaryOpFix.put("str", true);
		unaryOpFix.put("bound", false);
		
		aggregatorInternalVariable = null;
	}
	
	
	public void finishVisit() 
	{
	
	}

	
	public void startVisit() 
	{
	
	}

	
	public void visit(ExprFunction0 func) 
	{
		//System.out.println(" -- expr func0 " + func);
	}

	
	public void visit(ExprFunction1 func) 
	{
		System.out.println(" -- expr func1 " + func);
		String funcName = func.getFunctionName(null);
		
		// Languages
		if (funcName.equals("lang")) {
			func.getArg().visit(this);
			sqlExpr.append("_lang");
			return;
		}	
		
		
		sqlExpr.append("(");
		System.out.println("unaryOpFix = "+unaryOpFix);
		System.out.println("funcName = "+funcName);
		
		if (unaryOpFix.get(funcName)) 
		{
			sqlExpr.append(operatorMap.get(funcName));
			sqlExpr.append(" ");
			func.getArg().visit(this);
		}
		else 
		{
			func.getArg().visit(this);
			sqlExpr.append(" ");
			sqlExpr.append(operatorMap.get(funcName));
		}
		
		sqlExpr.append(")");
	}

	
	public void visit(ExprFunction2 func) {
		
		//System.out.println(" -- expr func2 " + func);
		
		// TODO
		if (func.getFunctionName(null).equals("langMatches")) 
		{
			sqlExpr.append("(");
			func.getArg1().visit(this);
			sqlExpr.append(" = LCASE(");
			func.getArg2().visit(this);
			sqlExpr.append("))");
			return;
		}
		
		Expr expr1 = func.getArg1();
		Expr expr2 = func.getArg2();
		//System.out.println(" -- expr1 " + expr1);
		//System.out.println(" -- expr2 " + expr2);
	
		
		if(expr1.isConstant() && expr1.getConstant().isIRI() && expr2.isVariable() )
		{ 
			sqlExpr.append("(");
			func.getArg1().visit(this);
			sqlExpr.append(" ");
			sqlExpr.append(operatorMap.get(func.getOpName()));
			sqlExpr.append(" ");
			func.getArg2().visit(this); 
			sqlExpr.append(")");
		}
		
		else if (expr1.isVariable() && expr2.isConstant() && expr2.getConstant().isIRI())
		{ 
			sqlExpr.append("(");
			func.getArg1().visit(this);
			sqlExpr.append(" ");
			sqlExpr.append(operatorMap.get(func.getOpName()));
			sqlExpr.append(" ");
			func.getArg2().visit(this);
			sqlExpr.append(")");
		}
		else
		{
			if(func.getOpName() == null && func.toString().contains("contains"))
			{
				sqlExpr.append("(CONTAINS(");
				func.getArg1().visit(this);
				sqlExpr.append(", ");
				func.getArg2().visit(this);
				sqlExpr.append(") > 0)");
			}
			else if(func.getOpName() != null)
			{
				sqlExpr.append("(");
				func.getArg1().visit(this);
				sqlExpr.append(" ");
				sqlExpr.append(operatorMap.get(func.getOpName()));
				sqlExpr.append(" ");
				func.getArg2().visit(this);
				sqlExpr.append(")");
			}
			else
			{
				try {
					throw new Exception("Function is null: "+func);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
		}
			
	}

	
	public void visit(ExprFunction3 func) 
	{
		//System.out.println(" -- expr func3 " + func);
	}

	
	public void visit(ExprFunctionN func) 
	{
		//System.out.println(" -- expr funcN " + func.getFunctionPrintName(null));
		
		//TODO HACK only support matching a substring
		if (func.getFunctionName(null).equals("regex")) 
		{
			sqlExpr.append("(");
			func.getArgs().get(0).visit(this);
			sqlExpr.append(" LIKE '%");
			sqlExpr.append(func.getArgs().get(1).getConstant().asUnquotedString());
			sqlExpr.append("%')");
		}
		
		// TODO. MORE HACK
		if (func.getFunctionPrintName(null).equals("<http://www.w3.org/2001/XMLSchema#double>")) 
		{
			func.getArgs().get(0).visit(this);
		}
	}

	
	public void visit(ExprFunctionOp funcOp) 
	{
		System.out.println(" -- expr funcOp " + funcOp);
	}

	
	public void visit(NodeValue nv) 
	{
		//System.out.println(" -- expr value " + nv);
		if (nv.isFloat() || nv.isInteger()) 
		{
			sqlExpr.append(nv.asUnquotedString());
		}
		else if (nv.isDate() || nv.isDateTime()) 
		{
			String val = nv.asUnquotedString().replace('T', '-').replace(':', '.');
			sqlExpr.append("'" + val + "'");
		}
		else 
		{
			sqlExpr.append("'" + nv.asUnquotedString().replace(baseURI, "") + "'");			
		}
	}

	
	public void visit(ExprVar nv) 
	{
		//System.out.println(" ++ expr var " + nv);
	
		if(!variables.contains(nv.getVarName()))
		{
			sqlExpr.append(nv);
			variables.add(nv.getVarName());
		}
		else
		{
			String varName = nv.getVarName();
			sqlExpr.append("?"+varName);
		}
	}

	
	public void visit(ExprAggregator eAgg) 
	{
		//System.out.println(" -- expr aggregator " + eAgg);
		
		Aggregator agg = eAgg.getAggregator();
		String key = agg.key();
		
		if(key.contains("count"))
		{
			sqlExpr.append("COUNT(");
			agg.getExpr().visit(this);
			sqlExpr.append(")");
		}
		else if(key.contains("sum"))
		{
			sqlExpr.append("SUM(");
			agg.getExpr().visit(this);
			sqlExpr.append(")");
		}
		else if(key.contains("min"))
		{
			sqlExpr.append("MIN(");
			agg.getExpr().visit(this);
			sqlExpr.append(")");
		}
		else if(key.contains("max"))
		{
			sqlExpr.append("MAX(");
			agg.getExpr().visit(this);
			sqlExpr.append(")");
		}
		else if(key.contains("avg"))
		{
			sqlExpr.append("AVG(");
			agg.getExpr().visit(this);
			sqlExpr.append(")");
		}
		
		ExprVar aggVar = eAgg.getAggVar();
		String internalVariable = aggVar.getVarName();
		this.aggregatorInternalVariable = internalVariable;
		
	}
	
	public String getAggregatorInternalVariable()
	{
		return aggregatorInternalVariable;
	}
	
	public String toString() 
	{
		return sqlExpr.toString();
	}

	public void resolveVariables(SqlOP q) 
	{
		String expr = sqlExpr.toString();
		for(String varname : variables) 
		{
			String resolvedVar = q.resolveVariable(varname);
			if(resolvedVar != null)
				expr = expr.replace("?" + varname, resolvedVar);	
		}
		sqlExpr = new StringBuilder(expr);
	}
	
	
	public List<String> getVariables() 
	{
		return variables;
	}
}