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


import com.drewshafer.sparql.backend.redis.ShardedRedisTripleStore;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class SPARQLEndpoint{

	
    public static void listen(Integer port, ShardedRedisTripleStore ts) throws Exception
    {
    	
       	Server server = new Server(port);

		ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);

        context.setContextPath("/");
        
        Executor threadPool = Executors.newCachedThreadPool();
        server.setHandler(context);
        
        try
        {
        	context.addServlet(new ServletHolder(new SPARQLServlet(ts)),"/sparql/*");
        	server.start();
            System.out.println("SPARQL Endpoint running at http://localhost:"+port+"/sparql");
            server.join();
        }
        catch(Exception e)
        {
        	 System.err.println("unable to connect to the database");
        }
    }

}