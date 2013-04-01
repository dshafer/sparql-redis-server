package main;

import com.beust.jcommander.Parameter;

public class Options {
	@Parameter(names = {"-p", "--populate"}, description = "RDF Input file to pre-polulate database")
	public String populate;
	
	@Parameter(names = {"-r", "--redis-config"}, description = "Redis server config file")
    public String redisConfig;
	
	@Parameter(names = {"-q", "--query-file"}, description = "Path to query file (")
	public String inputFile;
}
