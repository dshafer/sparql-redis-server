package main.server;

import java.io.IOException;
import java.net.URLDecoder;
import java.sql.SQLException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import main.RedisQueryExecution;
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
		//response.setCharacterEncoding("utf-8");
		ResultSet sparqlResult = executeSparql(sparqlQueryStr);
		String returnResponse = ResultSetFormatter.asXMLString(sparqlResult);
		String newResp = returnResponse.replaceAll("\n", "").replaceAll(">\\s*<", "><");;
		response.getWriter().write(newResp);
	}

	public ResultSet executeSparql(String sparql) throws SQLException, IOException, InterruptedException {
		QueryResult result = RedisQueryExecution.execute(sparql, ts);
		System.out.println(result.asTable());
		return result;
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
					+ "<form method=\"GET\" action=\"sparql\"/>"
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