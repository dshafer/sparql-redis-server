/*
 * (C) Copyright 2011 - Juan F. Sequeda and Daniel P. Miranker
 * Permission to use this code is only granted by separate license 
 */
package translate.sql;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.hp.hpl.jena.sparql.core.Var;


public class JOIN implements SqlOP 
{

	final int id;
	List<SqlOP> subOps = new LinkedList<SqlOP>();
	List<String> projectedVars = new LinkedList<String>();
	List<String> joinVars = new LinkedList<String>();
	List<SqlExpr> filters = new LinkedList<SqlExpr>();
	private long resultCount = 0;
	
	public JOIN(int id, SqlOP lhs, SqlOP rhs) 
	{
		
		this.id = id;
		
		if (lhs instanceof JOIN) 
		{
			subOps.addAll(((JOIN)lhs).subOps);
		}
		else 
		{
			subOps.add(lhs);
		}
		
		if (rhs instanceof JOIN) 
		{
			subOps.addAll(((JOIN)rhs).subOps);
		}
		else 
		{
			subOps.add(rhs);
		}
	}
	
	public String toString() 
	{
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		print(new PrintStream(os), 0);
		return os.toString();
	}

	
	public void projectVariable(String name) 
	{
		projectedVars.add(name);
		for (SqlOP q: subOps) 
		{
			if (q.getVariables().contains(name)) 
			{
				q.projectVariable(name);
			}
		}
	}

	
	public Set<String> getVariables() 
	{
		Set<String> vars = new TreeSet<String>();
		for (SqlOP q: subOps) 
		{
			vars.addAll(q.getVariables());
		}
		return vars;
	}


	public String resolveVariable(String name) 
	{
		for (SqlOP q: subOps) 
		{
			if (q.getVariables().contains(name)) 
			{
				return q.getName() + "." + name;
			}
		}
		return null;
	}

	
	public void addFilter(SqlExpr expr) 
	{
		// TODO Auto-generated method stub
		
	}

	
	public void print(PrintStream out, int indent) 
	{
		// TODO.  this loop mimics the print loop for the ON
		//        clause of the join statement.  Here we are 
		//        projecting the join variables in the subqueries
		//        before the are printed.
		for (int i = 0; i < subOps.size(); ++i) 
		{
			
			SqlOP op = subOps.get(i);
			
			if (i != 0) 
			{
				boolean foundJoinVar = false;
				for (String varname: op.getVariables()) 
				{
					for(int j = 0; j < i; ++j) 
					{
						SqlOP prevOp = subOps.get(j);
						Set<String> vars = prevOp.getVariables();
						if(vars.contains(varname) && varname.contains("_id")) 
						{
							foundJoinVar = true;
							op.projectJoinVariable(varname);
							prevOp.projectJoinVariable(varname);
						}
					}
				}
				
				if (!foundJoinVar) 
				{
					throw new UnsupportedOperationException("Cannot handle OPTIONAL patterns " +
							"that do not share variables with the containing graph pattern");
				}
			}
		}
		
//		for (SqlExpr filter: joinFilters) 
//		{
//			filter.resolveVariables(this);
//			for (String var: filter.getVariables()) 
//			{
//				for (SqlOP op : subOps) 
//				{
//					if (op.getVariables().contains(var)) 
//					{
//						op.projectVariable(var);
//						break;
//					}
//				}
//			}
//		}
		
		for (SqlExpr filter: filters) 
		{
			for (String var: filter.getVariables()) 
			{
				for (SqlOP op : subOps) 
				{
					if (op.getVariables().contains(var)) 
					{
						op.projectVariable(var);
						break;
					}
				}
			}
		}
		// -----
		
		boolean first = true;
		
		out.print("SELECT ");
//		if (distinct) 
//			out.print("DISTINCT ");
		
		if (resultCount > 0 ){
		
		}
		
		for (String varname : projectedVars) 
		{
			if (!first) 
			{
				out.println(",");
				PrintUtil.printSpace(out, indent);
				out.print("       ");
			}
			first = false;
			
			out.print(resolveVariable(varname) + " AS " + varname);
		}
		
		for (String varname : joinVars) 
		{
			if (!first) 
			{
				out.println(",");
				PrintUtil.printSpace(out, indent);
				out.print("       ");
			}
			first = false;
			out.print(resolveJoinVariable(varname) + " AS " + varname);
		}
		
		out.println();
		PrintUtil.printSpace(out, indent);
		out.println("FROM");
		for (int i = 0; i < subOps.size(); ++i) 
		{
			
			if (i != 0) 
			{
				out.println();
				PrintUtil.printSpace(out, indent);
				out.println("INNER JOIN");	
			}
			SqlOP q = subOps.get(i);
			
			PrintUtil.printSpace(out, indent + 3);
			out.print("(");
			q.print(out, indent + 3);
			out.print(") " + q.getName()); //REPLACING AS WITH A BLANK SPACE. THE 'AS' KEYWORD DOES NOT WORK IN ORACLE
			
			if (i != 0) 
			{
				out.println();
				PrintUtil.printSpace(out, indent);
				out.print("ON ");
				
				boolean foundJoinVar = false;
				for (String varname: q.getVariables()) 
				{
					for(int j = 0; j < i; ++j) 
					{
						
						SqlOP qj = subOps.get(j);
						Set<String> vars = qj.getVariables();
						
						//seems like a hack
						if(vars.contains(varname) && varname.contains("_id")) 
						{ 
							if (foundJoinVar) 
							{
								out.println(" AND");
								PrintUtil.printSpace(out, indent);
								out.print("   ");
							}
							foundJoinVar = true;
							out.print(qj.getName() + "." + varname);
							out.print(" = ");
							out.print(q.getName() + "." + varname);
							break;
						}
					}
				}
				
				if (!foundJoinVar) 
				{
					throw new UnsupportedOperationException("Cannot handle OPTIONAL patterns " +
							"that do not share variables with the containing graph pattern");
				}
			}
		}
		
//		// filters added to the ON clause
//		for (SqlExpr e: joinFilters) 
//		{
//			out.println(" AND");
//			PrintUtil.printSpace(out, indent);
//			out.print("   ");
//			out.print(e);
//		}
		
		// filters added to the WHERE clause
		if (filters.size() > 0) 
		{
			
			out.println();
			PrintUtil.printSpace(out, indent);
			out.print("WHERE ");
			for(int i = 0; i < filters.size(); ++i) 
			{
				if (i != 0)  
				{
					out.println(" AND");
					PrintUtil.printSpace(out, indent);
					out.print("      ");
				}
				out.print(filters.get(i));
			}
		}
		
//		if (includeOrder) 
//		{
//			out.println();
//			PrintUtil.printSpace(out, indent);
//			out.print(orderClause);
//		}
		

	}

	
	public String getName() 
	{
		return "join";
	}

	
	public void distinct() 
	{
		// TODO Auto-generated method stub
		
	}

	
	public void orderBy(SqlExpr expr, boolean up) 
	{
		/*
		 * TODO
		 
		if (includeOrder) {
			orderClause += ", ";
		}
		expr.resolveVariables(this);
		orderClause += expr + " " + (up ? "ASC" : "DESC") + " ";
		includeOrder = true;
		*/
	}

	
	public void slice(long start, long length) throws Exception 
	{
		
	}

	
	public void projectJoinVariable(String name) 
	{
		//System.out.println("JOIN: projectJoinVariable >> "+name);
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
				//return q.getName() + "." + name + "_id";
				String returnString = q.getName() + "." + name;
				return returnString;
			}
		}
		return null;
	}

	public void addAggregator(SqlExpr expr) {
		// TODO Auto-generated method stub
		System.err.println("addAggregator not implemented in JOIN");
	}

	public void addExtend(String internalVar,SqlExpr externalExpression) {
		// TODO Auto-generated method stub
		System.err.println("addExtend not implemented in JOIN");
	}

	public void addGroupBy(Var var) {
		// TODO Auto-generated method stub
		System.err.println("addGroupBy not implemented in JOIN");
	}
	
	public Map<String, SqlExpr> getAggregatorMap()  
	{
		return null;
	}

	public void addHaving(SqlExpr expr) {
		// TODO Auto-generated method stub
		System.err.println("addHaving not implemented in JOIN");
	}
}