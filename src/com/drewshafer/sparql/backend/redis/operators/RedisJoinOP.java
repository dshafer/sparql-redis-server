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
import com.hp.hpl.jena.sparql.expr.Expr;

public abstract class RedisJoinOP implements RedisOP {

	RedisOP lhs;
	RedisOP rhs;
	
	static public boolean canJoinInMapPhase(RedisOP lhs, RedisOP rhs){
		return false;
	}
	
	public RedisJoinOP(RedisOP _lhs, RedisOP _rhs){
		lhs = _lhs;
		rhs = _rhs;
	}
	
	@Override
	public String mapLuaScript() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		// TODO Auto-generated method stub
		return null;
	}

	public Boolean tryAddFilter(RedisExpressionVisitor rev) {
		Boolean result = lhs.tryAddFilter(rev);
		result |= rhs.tryAddFilter(rev);
		return result;
	}

	@Override
	public RedisOP tryConvertToJoin(RedisOP op, Boolean left) {
		RedisOP attempt = lhs.tryConvertToJoin(op, left);
		if(attempt != null){
			this.lhs = attempt;
			return this;
		}
		attempt = rhs.tryConvertToJoin(op, left);
		if(attempt != null){
			this.rhs = attempt;
			return this;
		}
		return null;
	}
	

	@Override
	public Set<String> getJoinVariables() {
		Set<String> result = lhs.getJoinVariables();
		result.addAll(rhs.getJoinVariables());
		return result;
	}
}
