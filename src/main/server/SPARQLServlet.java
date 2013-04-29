package main.server;

import java.io.IOException;
import java.net.URLDecoder;
import java.sql.SQLException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import main.ShardedRedisTripleStore;

import translate.redis.QueryResult;
import translate.sparql.SPARQLRedisVisitor;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;

public class SPARQLServlet extends HttpServlet {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6278335585909685759L;

	// public SPARQLServlet(){}
	private int counterEndpoint = 1;
	private int counterForm = 1;
	ShardedRedisTripleStore ts;

	public SPARQLServlet(ShardedRedisTripleStore _ts) {
		ts = _ts;
	}
	
	private void runQuery(String sparqlQueryStr, HttpServletResponse response) throws Exception {
		ResultSet sparqlResult = executeSparql(sparqlQueryStr);
		String returnResponse = ResultSetFormatter.asXMLString(sparqlResult);
		response.getWriter().write(returnResponse);
	}

	public ResultSet executeSparql(String sparql) throws SQLException, IOException, InterruptedException {
		Query q = QueryFactory.create(sparql);
		Op op = Algebra.compile(q);
		System.out.println("This is the Abstract Syntax Tree: ");
		System.out.println(op.toString());
		
		System.out.println("Walking the tree: ");
		SPARQLRedisVisitor v = new SPARQLRedisVisitor(ts);
		//SPARQLVisitor v = new SPARQLVisitor();
		OpWalker.walk(op, v);
		
		System.out.println("Translated query :\n" + v.toString());
		System.out.println("Map script is: \n" + v.luaMapScript());
		QueryResult result = ts.execute(v);
		
		result.unalias(ts);
		System.out.println(result.asTable());
		return result;
//		SparqlHiveLink link = new SparqlHiveLink();
//		
//		// Converts SPARQL query to HiveQL
//		String hiveql = link.convertToHive(sparql);
//		// System.out.println("\n\n"+hiveql);
//		String result = null;
//		String xmlresult = null;
//		ArrayList<String> projVars = null;
//		if (hiveql != null) {
//			System.out.println(hiveql);
//			// Executes the Hive Query
//			result = link.execute(hiveql);
//			System.out.println(result);
//			// Returns a String representing the SPARQL result in XML
//			projVars = link.getProjectedVars(sparql);
//			xmlresult = link.getXmlResult(projVars, result);
//		}
		//InputStream in = new ByteArrayInputStream(xmlresult.getBytes("UTF-8"));
//		return ResultSetFactory.fromXML(new String(""));
	}


	protected void doPost(HttpServletRequest req, HttpServletResponse response) throws ServletException, IOException {
		response.setContentType("application/sparql-results+xml");
		final String sparqlQueryStr = req.getParameter("query");
		// System.out.println("++++++++++ SPARQL POST Query no. "+counterForm);
		counterForm++;
		try {
			System.out.println("INPUT POST SPARQL QUERY =\n"+sparqlQueryStr);
			runQuery(sparqlQueryStr, response);
		} catch (Exception e) {
			System.err.println(e);
			response.getWriter().write(e.toString());
		}
	}

	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {

		response.setStatus(HttpServletResponse.SC_OK);
		String query = request.getParameter("query");
		if (query == null) {
			response.setContentType("text/html");
			String html = "<html><head><title>SPARQL Endpoint</title></head>"
					+ "<body>"
					+ "<h1>SPARQL Endpoint</h1>"
					+ "<form method=\"POST\" action=\"sparql\"/>"
					+ "<textarea rows=\"20\" cols=\"100\" name=\"query\" /></textarea>"
					+ "</br></br><input type=\"submit\" value=\"Submit SPARQL Query\" />"
					+ "</form>" + "</body>" + "</html>";
			response.getWriter().write(html);
		} else {
			response.setContentType("application/sparql-results+xml");
			final String sparqlQueryStr = URLDecoder.decode(query, "UTF-8");
			counterEndpoint++;
			try {
				System.out.println("INPUT GET SPARQL QUERY =\n"+sparqlQueryStr);
				runQuery(sparqlQueryStr, response);
			} catch (Exception e) {
				System.err.println(e);
				response.getWriter().write(e.toString());
			}

		}
	}

}