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

import com.drewshafer.sparql.backend.redis.RedisExpressionVisitor;
import com.hp.hpl.jena.sparql.expr.Expr;

public abstract class RedisProjectOP implements RedisOP{
	List<String> projectedVariables;
	protected RedisOP parent;
	
	public RedisProjectOP(RedisOP _parent){
		projectedVariables = new ArrayList<String>();
		this.parent = _parent;
		
	}
	
	public void projectVariable(String name) {
		projectedVariables.add('?' + name);
		
	}
	
	@Override
	public Boolean tryAddFilter(RedisExpressionVisitor rev) {
		return parent.tryAddFilter(rev);
	}

	@Override
	public RedisOP tryConvertToJoin(RedisOP op, Boolean left) {
		RedisOP attempt = parent.tryConvertToJoin(op, left);
		if(attempt != null){
			parent = attempt;
			return this;
		}
		return null;
	}

	@Override
	public Set<String> getJoinVariables() {
		return parent.getJoinVariables();
	}
}
