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

package io.r2dbc.postgresql.client;

import io.netty.channel.Channel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.kqueue.KQueue;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.authentication.PasswordAuthenticationHandler;
import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.backend.DataRow;
import io.r2dbc.postgresql.message.backend.NotificationResponse;
import io.r2dbc.postgresql.message.backend.RowDescription;
import io.r2dbc.postgresql.message.frontend.FrontendMessage;
import io.r2dbc.postgresql.message.frontend.Query;
import io.r2dbc.postgresql.util.PostgresqlServerExtension;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.test.StepVerifier;

import java.io.File;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assumptions.assumeThat;

abstract class AbstractProtocolConnectionTest {

    @RegisterExtension
    static final PostgresqlServerExtension SERVER = new PostgresqlServerExtension();

    protected abstract ProtocolConnectionProvider provider();

    protected abstract Channel getChannel(ProtocolConnection connection);

    protected final ProtocolConnection client = provider().connect(InetSocketAddress.createUnresolved(SERVER.getHost(), SERVER.getPort()), null, SSLConfig.disabled())
        .delayUntil(client -> StartupMessageFlow
            .exchange(this.getClass().getName(), m -> new PasswordAuthenticationHandler(SERVER.getPassword(), SERVER.getUsername()), client, SERVER.getDatabase(), SERVER.getUsername(),
                Collections.emptyMap()))
        .block();

    @AfterEach
    void closeClient() {
        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS test");
        this.client.close()
            .block();
    }

    @AfterEach
    void dropTable() {
        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS test");
    }


    @BeforeEach
    void createTable() {
        dropTable();
        SERVER.getJdbcOperations().execute("CREATE TABLE test ( value INTEGER )");
//        InetSocketAddress endpoint = InetSocketAddress.createUnresolved(SERVER.getHost(), SERVER.getPort());
    }

    @Test
    void close() {
        this.client.close()
            .thenMany(this.client.exchange(Mono.empty()))
            .as(StepVerifier::create)
            .verifyErrorSatisfies(t -> assertThat(t).isInstanceOf(R2dbcNonTransientResourceException.class).hasMessage("Cannot exchange messages because the connection is closed"));
    }

    @Test
    void disconnectedShouldRejectExchange() {
        getChannel(this.client).close().awaitUninterruptibly();

        this.client.close()
            .thenMany(this.client.exchange(Mono.empty()))
            .as(StepVerifier::create)
            .verifyErrorSatisfies(t -> assertThat(t).isInstanceOf(R2dbcNonTransientResourceException.class).hasMessage("Cannot exchange messages because the connection is closed"));
    }

    @Test
    void shouldCancelExchangeOnCloseFirstMessage() throws Exception {
        EmitterProcessor<FrontendMessage> messages = EmitterProcessor.create();
        Flux<BackendMessage> query = this.client.exchange(messages);
        CompletableFuture<List<BackendMessage>> future = query.collectList().toFuture();

        getChannel(this.client).eventLoop().execute(() -> {

            getChannel(this.client).close();
            messages.onNext(new Query("SELECT value FROM test"));
        });

        try {
            future.get(5, TimeUnit.SECONDS);
            fail("Expected PostgresConnectionClosedException");
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(R2dbcNonTransientResourceException.class).hasMessageContaining("Cannot exchange messages");
        }
    }

    @Test
    void shouldCancelExchangeOnCloseInFlight() throws Exception {
        EmitterProcessor<FrontendMessage> messages = EmitterProcessor.create();
        Flux<BackendMessage> query = this.client.exchange(messages);
        CompletableFuture<List<BackendMessage>> future = query.doOnNext(ignore -> {
            getChannel(this.client).disconnect();
            messages.onNext(new Query("SELECT value FROM test"));

        }).collectList().toFuture();

        messages.onNext(new Query("SELECT value FROM test;SELECT value FROM test;"));

        try {
            future.get(50, TimeUnit.SECONDS);
            fail("Expected PostgresConnectionClosedException");
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(R2dbcNonTransientResourceException.class).hasMessageContaining("Connection closed");
        }
    }

    @Test
    void constructorNoHost() {
        assertThatIllegalArgumentException().isThrownBy(() -> provider().connect(null, null, SSLConfig.disabled()))
            .withMessage("endpoint must not be null");
    }

    @Test
    void exchange() {
        SERVER.getJdbcOperations().execute("INSERT INTO test VALUES (100)");

        this.datarowCleanup(this.client
            .exchange(Mono.just(new Query("SELECT value FROM test"))))
            .as(StepVerifier::create)
            .assertNext(message -> assertThat(message).isInstanceOf(RowDescription.class))
            .assertNext(message -> assertThat(message).isInstanceOf(DataRow.class))
            .expectNext(new CommandComplete("SELECT", null, 1))
            .verifyComplete();
    }

    @Test
    void exchangeNoPublisher() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.client.exchange(null))
            .withMessage("requests must not be null");
    }

    @Test
    void handleBackendData() {
        assertThat(this.client.getProcessId()).isNotEmpty();
        assertThat(this.client.getSecretKey()).isNotEmpty();
    }

    @Test
    void handleParameterStatus() {
        Version version = this.client.getVersion();
        assertThat(version.getVersion()).isNotEmpty();
        assertThat(version.getVersionNumber()).isNotZero();
    }

    @Test
    void handleTransactionStatus() {
        assertThat(this.client.getTransactionStatus()).isEqualTo(TransactionStatus.IDLE);

        this.client
            .exchange(Mono.just(new Query("BEGIN")))
            .blockLast();

        assertThat(this.client.getTransactionStatus()).isEqualTo(TransactionStatus.OPEN);
    }

    @Test
    void handleTransactionStatusAfterCommand() {
        assertThat(this.client.getTransactionStatus()).isEqualTo(TransactionStatus.IDLE);

        this.datarowCleanup(this.client
            .exchange(Mono.just(new Query("SELECT value FROM test"))))
            .blockLast();

        assertThat(this.client.getTransactionStatus()).isEqualTo(TransactionStatus.IDLE);
    }

    @Test
    void handleTransactionStatusAfterCommit() {
        assertThat(this.client.getTransactionStatus()).isEqualTo(TransactionStatus.IDLE);

        this.client
            .exchange(Mono.just(new Query("BEGIN")))
            .blockLast();

        this.client
            .exchange(Mono.just(new Query("COMMIT")))
            .blockLast();

        assertThat(this.client.getTransactionStatus()).isEqualTo(TransactionStatus.IDLE);
    }

    @Test
    void largePayload() {
        IntStream.range(0, 1_000)
            .forEach(i -> SERVER.getJdbcOperations().update("INSERT INTO test VALUES(?)", i));

        this.datarowCleanup(this.client
            .exchange(Mono.just(new Query("SELECT value FROM test"))))
            .as(StepVerifier::create)
            .expectNextCount(1 + 1_000 + 1)
            .verifyComplete();
    }

    @Test
    void parallelExchange() {
        SERVER.getJdbcOperations().execute("INSERT INTO test VALUES (100)");
        SERVER.getJdbcOperations().execute("INSERT INTO test VALUES (1000)");

        this.datarowCleanup(this.client
            .exchange(Mono.just(new Query("SELECT value FROM test LIMIT 1"))))
            .zipWith(this.datarowCleanup(this.client.exchange(Mono.just(new Query("SELECT value FROM test LIMIT 1 OFFSET 1")))))
            .flatMapIterable(t -> Arrays.asList(t.getT1(), t.getT2()))
            .as(StepVerifier::create)
            .assertNext(message -> assertThat(message).isInstanceOf(RowDescription.class))
            .assertNext(message -> assertThat(message).isInstanceOf(RowDescription.class))
            .assertNext(message -> assertThat(message).isInstanceOf(DataRow.class))
            .assertNext(message -> assertThat(message).isInstanceOf(DataRow.class))
            .expectNext(new CommandComplete("SELECT", null, 1))
            .expectNext(new CommandComplete("SELECT", null, 1))
            .verifyComplete();
    }

    @Test
    void timeoutTest() {
        PostgresqlConnectionFactory postgresqlConnectionFactory = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
            .host("example.com")
            .port(81)
            .username("test")
            .password("test")
            .database(SERVER.getDatabase())
            .applicationName(AbstractProtocolConnectionTest.class.getName())
            .connectTimeout(Duration.ofMillis(200))
            .protocolConnectionProvider(provider())
            .build());

        postgresqlConnectionFactory.create()
            .as(StepVerifier::create)
            .expectError(R2dbcNonTransientResourceException.class)
            .verify(Duration.ofMillis(500));
    }

    @Test
    void unixDomainSocketTest() {

        String socket = "/tmp/.s.PGSQL.5432";

        assumeThat(KQueue.isAvailable() || Epoll.isAvailable()).describedAs("EPoll or KQueue must be available").isTrue();
        assumeThat(new File(socket)).exists();

        PostgresqlConnectionFactory postgresqlConnectionFactory = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
            .socket(socket)
            .username("postgres")
            .database(SERVER.getDatabase())
            .applicationName(AbstractProtocolConnectionTest.class.getName())
            .protocolConnectionProvider(provider())
            .build());

        postgresqlConnectionFactory.create()
            .flatMapMany(it -> {
                return it.createStatement("SELECT 1").execute().flatMap(r -> r.map((row, rowMetadata) -> row.get(0))).concatWith(it.close());
            })
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
    }

    @Test
    void handleNotify() {
        UnicastProcessor<NotificationResponse> response = UnicastProcessor.create();
        this.client.addNotificationListener(response::onNext);

        this.client
            .exchange(Mono.just(new Query("LISTEN events")))
            .blockLast();

        SERVER.getJdbcOperations().execute("NOTIFY events, 'test'");

        StepVerifier.create(response)
            .assertNext(message -> assertThat(message.getPayload()).isEqualTo("test"))
            .thenCancel()
            .verify();
    }

    @Test
    void handleTrigger() {
        SERVER.getJdbcOperations().execute(
            "CREATE OR REPLACE FUNCTION notify_event() RETURNS TRIGGER AS $$\n" +
                "  DECLARE\n" +
                "    payload JSON;\n" +
                "  BEGIN\n" +
                "    payload = row_to_json(NEW);\n" +
                "    PERFORM pg_notify('events', payload::text);\n" +
                "    RETURN NULL;\n" +
                "  END;\n" +
                "$$ LANGUAGE plpgsql;");

        SERVER.getJdbcOperations().execute(
            "CREATE TRIGGER notify_test_event\n" +
                "AFTER INSERT OR UPDATE OR DELETE ON test\n" +
                "  FOR EACH ROW EXECUTE PROCEDURE notify_event();");

        UnicastProcessor<NotificationResponse> response = UnicastProcessor.create();
        this.client.addNotificationListener(response::onNext);

        this.client
            .exchange(Mono.just(new Query("LISTEN events")))
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();

        SERVER.getJdbcOperations().execute("INSERT INTO test VALUES (100)");
        SERVER.getJdbcOperations().execute("INSERT INTO test VALUES (1000)");

        StepVerifier.create(response)
            .assertNext(message -> assertThat(message.getPayload()).isEqualTo("{\"value\":100}"))
            .assertNext(message -> assertThat(message.getPayload()).isEqualTo("{\"value\":1000}"))
            .thenCancel()
            .verify();
    }

    private Flux<BackendMessage> datarowCleanup(Flux<BackendMessage> in) {
        return Flux.create(sink -> Flux.from(in).subscribe(message -> {
            sink.next(message);
            ReferenceCountUtil.release(message);
        }, sink::error, sink::complete));
    }
}
