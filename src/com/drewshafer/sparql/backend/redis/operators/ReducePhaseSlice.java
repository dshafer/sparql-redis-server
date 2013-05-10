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

import java.util.Set;
import java.util.Stack;

import com.drewshafer.sparql.backend.redis.QueryResult;
import com.drewshafer.sparql.backend.redis.RedisExpressionVisitor;

public class ReducePhaseSlice implements RedisOP {

	RedisOP parent;
	long start;
	long length;
	public ReducePhaseSlice(RedisOP _parent, long _start, long _length){
		this.start = 0;
		if(_start != Long.MIN_VALUE){
			this.start = _start;
		}
		this.length = Long.MAX_VALUE;
		if(_length != Long.MIN_VALUE){
			this.length = _length;
		}
		this.parent = _parent;
	}
	
	@Override
	public String mapLuaScript() {
		return parent.mapLuaScript();
	}

	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		QueryResult pR = parent.reduce(patternStack);
		QueryResult result = new QueryResult(pR.columnNames);
		long prLim = this.start + pR.rows.size();
		for(long x = 0; (x < this.start + this.length) && (x < prLim) && (x < pR.rows.size()); x++){
			result.addRow(pR.rows.get((int)x));
		}
		return result;
	}

	@Override
	public Boolean completeAfterMapPhase() {
		return false;
	}

	@Override
	public String toString(String indent) {
		StringBuilder sb = new StringBuilder();
		sb.append("ReducePhaseSlice {\n");
		sb.append(indent + "  start : " + this.start + ",\n");
		sb.append(indent + "  length: " + this.length + ",\n");
		sb.append(indent + "  parent: " + parent.toString(indent + "  "));
		sb.append(indent + "}\n");
		return sb.toString();
	}

	@Override
	public Boolean tryAddFilter(RedisExpressionVisitor rev) {
		return parent.tryAddFilter(rev);
	}

	@Override
	public RedisOP tryConvertToJoin(RedisOP op, Boolean left) {
		RedisOP attempt = parent.tryConvertToJoin(op, left);
		if(attempt != null){
			this.parent = attempt;
			return this;
		}
		return null;
	}

	@Override
	public Set<String> getJoinVariables() {
		return parent.getJoinVariables();
	}

}
