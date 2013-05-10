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
package com.drewshafer.sparql.backend.redis.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import com.drewshafer.sparql.backend.redis.QueryResult;
import com.hp.hpl.jena.graph.Node;

public class ReducePhaseProject extends RedisProjectOP {
	String projectScript;
	
	public ReducePhaseProject(RedisOP _parent){
		super(_parent);
	}
	
	@Override
	public String mapLuaScript() {
		return parent.mapLuaScript();
	}


	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		QueryResult raw = parent.reduce(patternStack);
		
		List<Integer> projectedVarIndexes = new ArrayList<Integer>();
		for(String s: projectedVariables){
			boolean found = false;
			for(int varIdx = 0; varIdx < raw.columnNames.size(); varIdx++){
				if(s.equals(raw.columnNames.get(varIdx))){
					projectedVarIndexes.add(varIdx);
					found = true;
					break;
				}
				
			}
			if(!found){
				projectedVarIndexes.add(-1);
			}
		}
		QueryResult projected = new QueryResult(projectedVariables);
		for(List<Node> rawRow: raw.rows){
			List<Node> projectedRow = new ArrayList<Node>(rawRow.size());
			for(Integer varIdx: projectedVarIndexes){
				if(varIdx != -1){
					projectedRow.add(rawRow.get(varIdx));
				} else {
					projectedRow.add(Node.createLiteral(""));
				}
			}
			projected.addRow(projectedRow);
		}
		
		return projected;
	}
	
	@Override
	public Boolean completeAfterMapPhase() {
		return false;
	}

	@Override
	public String toString(String indent) {
		StringBuilder sb = new StringBuilder();
		sb.append("ReducePhaseProject {\n");
		sb.append(indent + "  projectVars: {");
		String delimiter = "";
		for(String projectedVariable: projectedVariables){
			sb.append(delimiter + "'" + projectedVariable + "'");
			delimiter = ",";
		}
		sb.append("}\n");
		sb.append(indent + "  parent : " + parent.toString(indent + "  "));
		return sb.toString();
	}



}
