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

public interface RedisOP {
	public String mapLuaScript();
	public QueryResult reduce(Stack<QueryResult> patternStack);
	public Boolean completeAfterMapPhase();
	public String toString(String indent);
	public Boolean tryAddFilter(RedisExpressionVisitor rev);
	public RedisOP tryConvertToJoin(RedisOP op, Boolean left);
	public Set<String> getJoinVariables();
}
