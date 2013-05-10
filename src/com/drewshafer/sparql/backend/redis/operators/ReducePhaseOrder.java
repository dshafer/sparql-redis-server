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
import java.util.Set;
import java.util.Stack;


import com.drewshafer.sparql.backend.redis.QueryResult;
import com.drewshafer.sparql.backend.redis.RedisExpressionVisitor;
import com.hp.hpl.jena.sparql.expr.Expr;


public class ReducePhaseOrder implements RedisOP{
	
	Expr expr;
	RedisOP parent;
	public ReducePhaseOrder(RedisOP _parent, Expr _expr){
		this.parent = _parent;
		this.expr = _expr;

	}
	
	@Override
	public String mapLuaScript() {
		return parent.mapLuaScript();

	}

	@Override
	public Boolean completeAfterMapPhase() {
		return false;
	}

	@Override
	public String toString(String indent) {
		StringBuilder sb = new StringBuilder();
		sb.append("ReducePhaseOrder{\n");
		sb.append(indent + "  expr  : " + expr.toString() + ",\n");
		sb.append(indent + "  parent: "+ parent.toString(indent + "    ") + "\n");
		sb.append(indent + "}");
		return sb.toString();
	}

	@Override
	public RedisOP tryConvertToJoin(RedisOP op, Boolean left) {
		RedisOP attempt = parent.tryConvertToJoin(op, left);
		if(attempt != null){
			parent = attempt;
			return this;
		} else {
			return null;
		}
	}

	@Override
	public Boolean tryAddFilter(RedisExpressionVisitor rev) {
		return parent.tryAddFilter(rev);
	}

	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		// TODO
		return parent.reduce(patternStack);
	}

	@Override
	public Set<String> getJoinVariables() {
		return parent.getJoinVariables();
	}
	
}
