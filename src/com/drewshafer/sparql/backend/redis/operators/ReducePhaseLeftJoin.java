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

import java.util.Stack;

import com.drewshafer.sparql.backend.redis.QueryResult;

public class ReducePhaseLeftJoin extends ReducePhaseJoin{

	public ReducePhaseLeftJoin(RedisOP _lhs, RedisOP _rhs) {
		super(_lhs, _rhs);
	}

	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		return super._reduce(patternStack, true);
	}
	
	@Override
	public String toString(String indent) {
		return "ReducePhaseLeftJoin {\n" +
				indent + "  left : " + lhs.toString(indent + "  ") + "\n" +
				indent + "  right: " + rhs.toString(indent + "  ") + "\n" +
				indent + "}";
	}
	
}
