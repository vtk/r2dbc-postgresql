/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql;

import io.r2dbc.postgresql.client.Binding;
import io.r2dbc.postgresql.client.ProtocolConnection;
import io.r2dbc.postgresql.client.ExtendedQueryMessageFlow;
import io.r2dbc.postgresql.util.Assert;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

final class IndefiniteStatementCache implements StatementCache {

    private final Map<Tuple2<String, List<Integer>>, Mono<String>> cache = new HashMap<>();

    private final ProtocolConnection protocolConnection;

    private final AtomicInteger counter = new AtomicInteger();

    IndefiniteStatementCache(ProtocolConnection protocolConnection) {
        this.protocolConnection = Assert.requireNonNull(protocolConnection, "client must not be null");
    }

    @Override
    public Mono<String> getName(Binding binding, String sql) {
        Assert.requireNonNull(binding, "binding must not be null");
        Assert.requireNonNull(sql, "sql must not be null");

        return this.cache.computeIfAbsent(Tuples.of(sql, binding.getParameterTypes()),
            tuple -> this.parse(tuple.getT1(), tuple.getT2()));
    }

    @Override
    public String toString() {
        return "IndefiniteStatementCache{" +
            "cache=" + this.cache +
            ", client=" + this.protocolConnection +
            ", counter=" + this.counter +
            '}';
    }

    private Mono<String> parse(String sql, List<Integer> types) {
        String name = String.format("S_%d", this.counter.getAndIncrement());

        ExceptionFactory factory = ExceptionFactory.withSql(name);
        return ExtendedQueryMessageFlow
            .parse(this.protocolConnection, name, sql, types)
            .handle(factory::handleErrorResponse)
            .then(Mono.just(name))
            .cache();
    }

}
