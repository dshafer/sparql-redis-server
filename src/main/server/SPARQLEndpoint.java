package main.server;

import main.ShardedRedisTripleStore;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class SPARQLEndpoint{

	
    public static void listen(Integer port, ShardedRedisTripleStore ts) throws Exception
    {
    	

    	Server server = new Server(port);
		ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);

        context.setContextPath("/");
      
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