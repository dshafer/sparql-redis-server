package main;

import com.beust.jcommander.Parameter;

public class Options {
	@Parameter(names = {"-p", "--populate"}, description = "RDF Input file to pre-polulate database")
	public String populate;
	
	@Parameter(names = {"-r", "--redis-config"}, description = "Redis server config file")
    public String redisConfig;
	
	@Parameter(names = {"-q", "--query-file"}, description = "Path to query file (")
	public String inputFile;
	
	@Parameter(names = {"-l", "--listen"}, description = "Start a SPARQL endpoint instead of running a canned query (default=false)")
	public String listen = "false";
	
	@Parameter(names = {"-lp", "--port"}, description = "Port assignment for SPARQL endpoint (default=8080)")
	public Integer listenPort = 8080;
}
