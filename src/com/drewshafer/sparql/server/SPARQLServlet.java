/**
 * Copyright (C) 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.drewshafer.sparql.server;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.drewshafer.sparql.RedisQueryExecution;
import com.drewshafer.sparql.backend.redis.ShardedRedisTripleStore;

import com.drewshafer.sparql.backend.redis.QueryResult;
import com.hp.hpl.jena.query.*;

public class SPARQLServlet extends HttpServlet {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6278335585909685759L;
	private int counterEndpoint = 1;
	private int counterForm = 1;
	ShardedRedisTripleStore ts;
	
	public SPARQLServlet(ShardedRedisTripleStore ts2) {
		ts = ts2;
	}
	
	private void runQuery(String sparqlQueryStr, HttpServletResponse response) throws Exception {
		String returnResponse = null;
		try{
			ResultSet sparqlResult = executeSparql(sparqlQueryStr);
			long startTime = System.currentTimeMillis();
			returnResponse = ResultSetFormatter.asXMLString(sparqlResult);
			System.out.println("Convert to XML : " + (double)(System.currentTimeMillis() - startTime)/1000 + " seconds");
		} catch (Exception e){
			System.out.println("ERROR DURING QUERY EXECUTION: " + e.getMessage() + "\n");
			e.printStackTrace();
			returnResponse = null;
		}
		response.getWriter().write(returnResponse);
		response.getWriter().close();
	}
	
	public ResultSet executeSparql(String sparql) throws SQLException, IOException, InterruptedException {
		QueryResult result = RedisQueryExecution.execute(sparql, ts);
		System.out.println("returning " + result.rows.size() + " rows.");
//		System.out.println(result.asTable());
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
					+ "</form>" 
					+ "<a href='?shutdown=yes'>Shut Down</a>"
					+ "</body>" 
					+ "</html>";
			response.getWriter().write(html);
		} else {
			response.setContentType("application/sparql-results+xml");
			counterEndpoint++;
			try {
				System.out.println("\n\n\n===============================================\n");
				System.out.println("Time: " + (new Date()).toString());
				System.out.println("INPUT GET SPARQL QUERY =\n"+query);
				runQuery(query, response);
			} catch (Exception e) {
				System.err.println(e);
				response.getWriter().write(e.toString());
			}
	
		}
	}

}