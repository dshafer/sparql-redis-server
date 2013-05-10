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
package com.drewshafer.sparql;

import com.beust.jcommander.Parameter;

public class Options {
	@Parameter(names = {"-p", "--populate"}, description = "RDF Input file to pre-polulate database")
	public String populate;
	
	@Parameter(names = {"-r", "--redis-config"}, description = "Redis cluster config file")
    public String redisClusterConfig;
	
	@Parameter(names = {"-rb", "--redis-base-config"}, description = "Redis base server.conf file")
    public String redisBaseConfig;
	
	@Parameter(names = {"-q", "--query-file"}, description = "Path to query file (")
	public String inputFile;
	
	@Parameter(names = {"-l", "--listen"}, description = "Start a SPARQL endpoint instead of running a canned query (default=false)")
	public Boolean  listen = false;
	
	@Parameter(names = {"-lp", "--port"}, description = "Port assignment for SPARQL endpoint (default=8080)")
	public Integer listenPort = 8080;
	
	@Parameter(names = {"-d", "--data-dir"}, description = "Datafile Directory")
	public String dataDir = "./data";
	
	@Parameter(names = {"-s", "--start-redis"}, description = "Attempt to start redis-server instances described in cluster config file")
	public Boolean startRedisServers = false;
	
	@Parameter(names = {"-h", "--help"}, description="print help and exit")
	public Boolean getHelp = false;
	
	@Parameter(names = {"-tp", "--test-parse"}, description="don't connect to redis, just test-parse the input data")
	public Boolean testParse = false;
		
}
