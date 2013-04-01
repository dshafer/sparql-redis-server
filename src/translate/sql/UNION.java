/*
 * (C) Copyright 2011 - Juan F. Sequeda and Daniel P. Miranker
 * Permission to use this code is only granted by separate license 
 */
package translate.sql;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.hp.hpl.jena.sparql.core.Var;

public class UNION implements SqlOP 
{

	int id;
	List<SqlOP> subOps = new LinkedList<SqlOP>();
	Set<String> projectedVars = new LinkedHashSet<String>();
	Set<String> joinVars = new LinkedHashSet<String>();
	
	private boolean distinct = false;
	private long resultCount = 0;
	private String orderClause = "ORDER BY ";
	private boolean includeOrder;
	
	public UNION(int id, SqlOP lhs, SqlOP rhs) 
	{
		this.id = id;
		
		// if any of the subqueries is an union, merge it 
		// with this top level union
		if (lhs instanceof UNION) 
		{
			subOps.addAll(((UNION)lhs).subOps);
		}
		else 
		{
			subOps.add(lhs);
		}
		
		if (rhs instanceof UNION) 
		{
			subOps.addAll(((UNION)rhs).subOps);
		}
		else 
		{
			subOps.add(rhs);
		}
	}

	
	public void projectVariable(String name) 
	{
		projectedVars.add(name);
		for (SqlOP op: subOps) 
		{
			op.projectVariable(name);
		}
	}
	
	public String toString() 
	{
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		print(new PrintStream(os), 0);
		return os.toString();
	}

	
	public Set<String> getVariables() 
	{
		Set<String> variables = new TreeSet<String>();
		for (SqlOP op: subOps) 
		{
			variables.addAll(op.getVariables());
		}
		return variables;
	}

	
	public String resolveVariable(String name) 
	{
		for (SqlOP op: subOps) 
		{
			if (op.getVariables().contains(name)) 
			{
				//return q.getName() + "." + name;
				return name;
			}
		}
		return null;
	}

	
	public void addFilter(SqlExpr expr) 
	{
		// TODO. how to handle NULLs of variables in the filter expression.
		throw new UnsupportedOperationException("TODO. cannot handle FILTER outside UNION");
	}

	
	public void print(PrintStream out, int indent) 
	{
		
		boolean generateInitialSelect = true;
	
		
		if(generateInitialSelect)
		{
			//The following output does not work in MSSQL or POSTGRESQL for a UNION.
			// I THINK THERE IS AN ISSUE WITH THE LACK OF DISTINCT POSSIBLE
			boolean first = true;
			out.print("SELECT ");
			
			if (distinct) 
				out.print("DISTINCT ");
			
			for (String var : projectedVars) 
			{
				if (!first) 
				{
					out.println(",");
					PrintUtil.printSpace(out, indent);
					out.print("       ");
				}
				first = false;
				// out.print(resolveVariable(var) + " AS " + var);
				out.print(var);
			}

			out.println();
			PrintUtil.printSpace(out, indent);
			out.println("FROM");
		}
		
		for (int i = 0; i < subOps.size(); ++i) 
		{
			if (i != 0) 
			{
				out.println();
				PrintUtil.printSpace(out, indent);
				out.println("UNION");
				
			}
			
			SqlOP op = subOps.get(i);
			PrintUtil.printSpace(out, indent + 3);
			out.print("(");
			op.print(out, indent + 3);
			//out.print(") AS " + q.getName());
			out.print(")");
		}
		
		/*
		if (filters.size() > 0) {
			out.println();
			PrintUtil.printSpace(out, indent);
			out.print("WHERE ");
			for(int i = 0; i < filters.size(); ++i) {
				if (i != 0)  {
					out.println(" AND");
					PrintUtil.printSpace(out, indent);
					out.print("      ");
				}
				Expression filt = filters.get(i);
				out.print(filt);
				
				TODO. need to add 'var IS NULL?' for every
				      variable in the filter expression?
			}
		}
		*/
	
		if (includeOrder) 
		{
			out.println();
			PrintUtil.printSpace(out, indent);
			out.print(orderClause);
		}
		
		if (resultCount > 0)
		{
			//this is for limit offset
			/*
			out.println();
			PrintUtil.printSpace(out, indent);
			out.println("" +
					" FIRST " + resultCount + " ROWS ONLY");
			PrintUtil.printSpace(out, indent);
			out.print("OPTIMIZE FOR " + resultCount + " ROWS");
			*/
		}
	}

	
	public String getName() 
	{
		return "union" + id;
	}

	
	public void distinct() 
	{
		distinct = true;
	}

	
	public void orderBy(SqlExpr expr, boolean up) 
	{
		if (includeOrder) 
		{
			orderClause += ", ";
		}
		expr.resolveVariables(this);
		orderClause += expr + " " + (up ? "ASC" : "DESC") + " ";
		includeOrder = true;
	}

	
	public void slice(long start, long length) 
	{
		resultCount = length;
	}

	
	public void projectJoinVariable(String name) 
	{
		joinVars.add(name);
		for (SqlOP q: subOps) 
		{
			if (q.getVariables().contains(name)) 
			{
				q.projectJoinVariable(name);
			}
		}
	}

	
	public String resolveJoinVariable(String name) 
	{
		for (SqlOP q: subOps) 
		{
			if (q.getVariables().contains(name)) 
			{
				return q.getName() + "." + name + "_id";
			}
		}
		return null;
	}


	public void addAggregator(SqlExpr expr) {
		// TODO Auto-generated method stub
		System.err.println("addAggregator not implemented in UNION");
	}


	public void addExtend(String internalVar, SqlExpr externalExpression) {
		// TODO Auto-generated method stub
		System.err.println("addExtend not implemented in UNION");
	}


	public void addGroupBy(Var var) {
		// TODO Auto-generated method stub
		System.err.println("addGroupBy not implemented in UNION");
	}
	
	public Map<String, SqlExpr> getAggregatorMap() 
	{
		return null;
	}


	public void addHaving(SqlExpr expr) {
		// TODO Auto-generated method stub
		System.err.println("addHaving not implemented in UNION");
	}
}