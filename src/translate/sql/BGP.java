/*
 * (C) Copyright 2011 - Juan F. Sequeda and Daniel P. Miranker
 * Permission to use this code is only granted by separate license 
 */
package translate.sql;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Var;


public class BGP implements SqlOP {


	class Term 
	{
		String table;
		String type;
		String value;
		
		Term(String table, String type, String value) 
		{
			this.table = table;
			this.type = type;
			this.value = value;
			if(this.value != null)
				this.value = value.replace("%20", " "); // Replace %20 with a space. i.e 'genomic%20RNA' --> 'genomic RNA'
		}
		
		public String toString() 
		{
			return table + "." + type + (value == null ? "" : " = " + value);
		}
	}
	

	
	final int id;
	Map<String,List<Term>> variableMap = new TreeMap<String,List<Term>>();
	Map<String, SqlExpr> aggregatorMap = new HashMap<String, SqlExpr>();
	Map<String, SqlExpr> extendMap = new HashMap<String, SqlExpr>();
	List<String> tables = new LinkedList<String>();
	Map<String, String> viewMap = new TreeMap<String, String>();
	Set<String> projectedVars = new LinkedHashSet<String>();
	Set<String> joinVars = new LinkedHashSet<String>();
	List<Term> terms = new LinkedList<Term>();
	List<SqlExpr> filters = new LinkedList<SqlExpr>();

	private boolean distinct = false;
	private long resultCount = 0;
	private String orderClause = "ORDER BY ";
	//private String aggregateClause = "";
	private String groupByClause = "GROUP BY ";
	private boolean includeOrder;
	//private boolean includeAggregate;
	private boolean includeGroupBy;
	private boolean includeHaving;
	private String havingClause = "HAVING ";
	
	public BGP(int id) 
	{
		this.id = id;
	}
	
	public void addTriple(int tripleId, Triple t) throws Exception 
	{
		
		String tableAlias = "t" + tripleId;
		String prpName = null;
		
		tables.add(tableAlias);
		
		Node subNode = t.getSubject();
		Node prpNode = t.getPredicate();
		Node objNode = t.getObject();
		
		if (!subNode.isVariable())
		{
			terms.add(new Term(tableAlias, "s", "'" + subNode.getURI() + "'"));
		
		}
		else 
		{
			String varName = subNode.getName();
			Term term = new Term(tableAlias, "s", null);
			addVariable(varName, term, null);
		
			
		}

		//---------------
		if (!prpNode.isVariable())
		{
			prpName = prpNode.getURI();
			terms.add(new Term(tableAlias, "p", "'" + prpNode.getURI() + "'"));
		}
		else 
		{
			addVariable(prpNode.getName(), new Term(tableAlias, "p", null), null);
		}
		
		//---------------
		if (!objNode.isVariable())
		{
			// inspect the object to determine its datatype
			if (objNode.isURI()) 
			{
				String obj = "'"+objNode.getURI()+"'";
				terms.add(new Term(tableAlias, "o", obj));
			}
			else if (objNode.isLiteral()) 
			{
				String obj = "'" + objNode.getLiteralLexicalForm() + "'";
				terms.add(new Term(tableAlias, "o", obj));
			}
			
		}
		else 
		{
			String varName = objNode.getName();
			Term term = new Term(tableAlias, "o", null);
			addVariable(varName, term, prpName);
		}
		
		// The predicate is a constant so link the table alias to the specific table datatype
		if (prpName != null) 
			viewMap.put(tableAlias, "tripleview"); 
	}
	
	public void addVariable(String varname, Term term, String prpName) throws Exception 
	{
		List<Term> list = variableMap.get(varname);
		if (list == null) 
		{
			list = new LinkedList<Term>();
			variableMap.put(varname, list);
		}
		list.add(term);
	}
	
	
	public void print(PrintStream out, int indent) 
	{

		boolean f = true;
		
		out.print("SELECT ");
		if (distinct) 
			out.print("DISTINCT ");
		
		if(projectedVars.size() == 0 && joinVars.size() == 0)
		{
			Set<String> variables = getVariables();
			if(variables.size() == 0)
			{
				//No variables in the BGP, therefore just do "SELECT * ... "
				out.print(" * "	);
			}
			else
			{
				//There are variables in the BGP
				for (String var : variables) 
				{
					if (!f) 
					{ 
						out.println(",");
						PrintUtil.printSpace(out, indent);
						out.print("       "); 
					}
					f = false;
					out.print(resolveVariable(var) + " AS " + var	);
				}
			}
			
			out.println();
		}
		else
		{
			for (String var : projectedVars) 
			{
				if (!f) 
				{ 
					out.println(",");
					PrintUtil.printSpace(out, indent);
					out.print("       "); 
				}
				f = false;
				out.print(resolveVariable(var) + " AS " + var	);
			}
			
			for (String var : joinVars) 
			{
				if (!f) 
				{ 
					out.println(",");
					PrintUtil.printSpace(out, indent);
					out.print("       "); 
				}
				f = false;
				out.print(resolveVariable(var) + " AS " + var);
			}
			out.println();
		}
		
		f = true;
		PrintUtil.printSpace(out, indent);
		out.print("FROM ");
		for (String t : tables) 
		{
			if (!f)  
			{
				out.println(",");
				PrintUtil.printSpace(out, indent);
				out.print("     "); 
			}
			f = false;
			out.print(viewMap.get(t));
			out.print(" " + t);
		}
		out.println();
		
		f = true;
		PrintUtil.printSpace(out, indent);
		out.print("WHERE ");
		for (Term t : terms) 
		{
			if (!f)  
			{
				out.println(" AND");
				PrintUtil.printSpace(out, indent);
				out.print("      ");
			}
			f = false;
			out.print(t);
		}
		
		for(List<Term> l : variableMap.values()) 
		{
			
			if (l.size() <= 1)
				continue;
		
			for (int i = 0; i < l.size() - 1; ++i) 
			{
				Term leftTerm = l.get(i);
				Term rightTerm = l.get(i + 1);
				if (!f)  
					{
						out.println(" AND");
						PrintUtil.printSpace(out, indent);
						out.print("      ");
					}
					f = false;
					out.print(leftTerm + " = " + rightTerm);
				
				
			}
		}
		for(SqlExpr filter : filters) {
			
			if (!f)  {
				out.println(" AND");
				PrintUtil.printSpace(out, indent);
				out.print("      ");
			}
			f = false;
			out.print(filter);
		}
		if (resultCount > 0) {
				
		}
		
		if (includeGroupBy) 
		{
			out.println();
			PrintUtil.printSpace(out, indent);
			out.print(groupByClause);
		}
		if(includeHaving)
		{
			out.println();
			PrintUtil.printSpace(out, indent);
			out.print(havingClause);
		}
		
		if (includeOrder) 
		{
			out.println();
			PrintUtil.printSpace(out, indent);
			out.print(orderClause);
		}
	}
	
	public void projectVariable(String var) 
	{
		projectedVars.add(var);
	}
	
	public void print(int indent) 
	{
		print(System.out, indent);
	}
	
	public String resolveVariable(String varname) 
	{

		if (variableMap.containsKey(varname)) 
		{
			return variableMap.get(varname).get(0).toString();
		}
		else if(extendMap.containsKey(varname))
		{
			return extendMap.get(varname).toString();
		}
		else if(aggregatorMap.containsKey(varname))
		{
			return aggregatorMap.get(varname).toString();
		}
		else
		{
			return null;
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
		return variableMap.keySet();
	}

	public void addFilter(SqlExpr expr) 
	{
		expr.resolveVariables(this);
		filters.add(expr);
	}

	public String getName() 
	{
		return "bgp" + id;
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

	public void slice(long start, long length) throws Exception 
	{
;
	}

	public void projectJoinVariable(String name) 
	{
		joinVars.add(name);
	}

	public String resolveJoinVariable(String name) 
	{
		return resolveVariable(name) + "_id";
	}

	public void addAggregator(SqlExpr expr) 
	{
		String id = expr.getAggregatorInternalVariable();
		expr.resolveVariables(this);
		aggregatorMap.put(id, expr);
	}
	
	public void addExtend(String internalVar, SqlExpr externalExpr)
	{
		externalExpr.resolveVariables(this);
		extendMap.put(internalVar, externalExpr);
	}

	public void addGroupBy(Var var) 
	{
		if (includeGroupBy) 
		{
			groupByClause += ", ";
		}
		groupByClause += resolveVariable(var.getVarName()) + " ";
		includeGroupBy = true;
	}
	
	 
	
	public void addHaving(SqlExpr expr)
	{
		expr.resolveVariables(this);
		if (includeHaving) 
		{
			havingClause += ", ";
		}
		havingClause += expr + " ";
		includeHaving = true;
	}

	public Map<String, SqlExpr> getAggregatorMap() 
	{
		return aggregatorMap;
	}

}