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
import java.util.Set;
import java.util.Stack;

import com.drewshafer.sparql.backend.redis.QueryResult;
import com.hp.hpl.jena.sparql.expr.Expr;

public abstract class RedisFilterOP implements RedisOP{
	
	List<Expr> expressions;
	RedisOP parent;
	public RedisFilterOP(RedisOP _parent){
		expressions = new ArrayList<Expr>();
		this.parent = _parent;
	}
	
	@Override
	public String mapLuaScript() {
		return parent.mapLuaScript();

	}
	
	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		return parent.reduce(patternStack);
	}

	public void addFilter(Expr e){
		expressions.add(e);
	}

	@Override
	public Set<String> getJoinVariables() {
		return parent.getJoinVariables();
	}
	
}
