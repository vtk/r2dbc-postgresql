package io.r2dbc.postgresql;

import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.client.ProtocolConnectionProvider;
import io.r2dbc.postgresql.client.SSLMode;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import java.io.File;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class AbstractConnectionFactoryIntegrationTests extends AbstractIntegrationTests {

    abstract ProtocolConnectionProvider provider();

    @Override
    protected void customize(PostgresqlConnectionConfiguration.Builder builder) {
        builder.protocolConnectionProvider(provider());
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    final class ScramIntegrationTests {

        @Test
        void scramAuthentication() {
            createConnectionFactory("test-scram", "test-scram").create()
                .flatMapMany(c -> c.createStatement("SELECT 1 test").execute())
                .flatMap(r -> r.map((row, meta) -> row.get(0)))
                .as(StepVerifier::create)
                .expectNextCount(1)
                .verifyComplete();
        }

        @Test
        void scramAuthenticationFailed() {
            createConnectionFactory("test-scram", "wrong").create()
                .as(StepVerifier::create)
                .verifyError(R2dbcPermissionDeniedException.class);
        }

        private PostgresqlConnectionFactory createConnectionFactory(String username, String password) {
            return getConnectionFactory(b -> b.username(username).password(password));
        }
    }


    public static class FailedVerification implements HostnameVerifier {

        @Override
        public boolean verify(String s, SSLSession sslSession) {
            return false;
        }
    }

    public static class NoVerification implements HostnameVerifier {

        @Override
        public boolean verify(String s, SSLSession sslSession) {
            return true;
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    final class SslIntegrationTests {

        @Test
        void exchangeSslWithClientCert() {
            client(
                c -> c,
                c -> c.map(client -> client.createStatement("SELECT 10")
                    .execute()
                    .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
                    .as(StepVerifier::create)
                    .expectNext(10)
                    .verifyComplete()));
        }

        @Test
        void exchangeSslWithClientCertInvalidClientCert() {
            client(
                c -> c.sslRootCert(SERVER.getServerCrt())
                    .sslCert(SERVER.getServerCrt())
                    .sslKey(SERVER.getServerKey()),
                c -> c
                    .as(StepVerifier::create)
                    .expectError(R2dbcPermissionDeniedException.class)
                    .verify());
        }

        @Test
        void exchangeSslWithClientCertNoCert() {
            client(
                c -> c
                    .password("test"),
                c -> c
                    .as(StepVerifier::create)
                    .expectError(R2dbcPermissionDeniedException.class));
        }

        @Test
        void exchangeSslWithPassword() {
            client(
                c -> c
                    .sslRootCert(SERVER.getServerCrt())
                    .username("test-ssl")
                    .password("test-ssl"),
                c -> c.map(client -> client.createStatement("SELECT 10")
                    .execute()
                    .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
                    .as(StepVerifier::create)
                    .expectNext(10)
                    .verifyComplete()));
        }

        @Test
        void invalidServerCertificate() {
            client(
                c -> c
                    .sslRootCert(SERVER.getClientCrt()),
                c -> c
                    .as(StepVerifier::create)
                    .expectError(R2dbcNonTransientResourceException.class)
                    .verify());
        }

        @Test
        void userIsNotAllowedToLoginWithSsl() {
            client(
                c -> c.sslRootCert(SERVER.getServerCrt())
                    .username(SERVER.getUsername())
                    .password(SERVER.getPassword())
                ,
                c -> c
                    .as(StepVerifier::create)
                    .verifyError(R2dbcPermissionDeniedException.class));
        }

        @Test
        void userIsNotAllowedToLoginWithoutSsl() {
            client(
                c -> c.sslMode(SSLMode.DISABLE)
                    .username("test-ssl")
                    .password("test-ssl"),
                c -> c
                    .as(StepVerifier::create)
                    .verifyError(R2dbcPermissionDeniedException.class));
        }

        private void client(Function<PostgresqlConnectionConfiguration.Builder, PostgresqlConnectionConfiguration.Builder> configurer,
                            Consumer<Mono<io.r2dbc.postgresql.api.PostgresqlConnection>> connectionConsumer) {
            getConnectionFactory(b -> {
                b.username("test-ssl-with-cert")
                    .password(null)
                    .sslMode(SSLMode.VERIFY_FULL)
                    .sslHostnameVerifier(new NoVerification());
                configurer.apply(b);
            })
                .create()
                .onErrorResume(e -> Mono.fromRunnable(() -> connectionConsumer.accept(Mono.error(e))))
                .delayUntil(connection -> Mono.fromRunnable(() -> connectionConsumer.accept(Mono.just(connection))))
                .flatMap(PostgresqlConnection::close)
                .block();
        }

        @Nested
        class SslModes {

            @Test
            void allowFailed() {
                client(
                    c -> c
                        .sslMode(SSLMode.ALLOW)
                        .username("test-ssl")
                    ,
                    c -> c
                        .as(StepVerifier::create)
                        .verifyError(R2dbcPermissionDeniedException.class));
            }

            @Test
            void allowFallbackToSsl() {
                client(
                    c -> c
                        .sslMode(SSLMode.ALLOW)
                        .username("test-ssl")
                        .password("test-ssl")
                    ,
                    c -> c
                        .as(StepVerifier::create)
                        .expectNextCount(1)
                        .verifyComplete());
            }

            @Test
            void allowFallbackToSslAndFailedAgain() {
                client(
                    c -> c
                        .sslMode(SSLMode.ALLOW)
                        .username("test-ssl-test")
                        .password("test-ssl")
                    ,
                    c -> c
                        .as(StepVerifier::create)
                        .verifyError(R2dbcPermissionDeniedException.class));
            }

            @Test
            void allowWithoutSsl() {
                client(
                    c -> c
                        .sslMode(SSLMode.ALLOW)
                        .username(SERVER.getUsername())
                        .password(SERVER.getPassword())
                    ,
                    c -> c
                        .as(StepVerifier::create)
                        .expectNextCount(1)
                        .verifyComplete());
            }

            @Test
            void disabled() {
                client(
                    c -> c
                        .sslMode(SSLMode.DISABLE)
                        .username(SERVER.getUsername())
                        .password(SERVER.getPassword()),
                    c -> c
                        .as(StepVerifier::create)
                        .expectNextCount(1)
                        .verifyComplete());
            }

            @Test
            void disabledFailedForSslOnlyUser() {
                client(
                    c -> c
                        .sslMode(SSLMode.DISABLE)
                        .password("test-ssl")
                        .password("test-ssl")
                    ,
                    c -> c
                        .as(StepVerifier::create)
                        .expectError(R2dbcPermissionDeniedException.class)
                        .verify());
            }

            @Test
            void preferFallbackToTcp() {
                client(
                    c -> c
                        .sslMode(SSLMode.PREFER)
                        .username(SERVER.getUsername())
                        .password(SERVER.getPassword())
                    ,
                    c -> c
                        .as(StepVerifier::create)
                        .expectNextCount(1)
                        .verifyComplete());
            }

            @Test
            void preferFallbackToTcpAndFailed() {
                client(
                    c -> c
                        .sslMode(SSLMode.PREFER)
                        .username("test-ssl-test")
                        .password(SERVER.getPassword())
                    ,
                    c -> c
                        .as(StepVerifier::create)
                        .expectError(R2dbcPermissionDeniedException.class));
            }

            @Test
            void preferWithSsl() {
                client(
                    c -> c
                        .sslMode(SSLMode.PREFER)
                        .sslKey(SERVER.getClientKey())
                        .sslCert(SERVER.getClientCrt()),
                    c -> c
                        .as(StepVerifier::create)
                        .expectNextCount(1)
                        .verifyComplete());
            }

            @Test
            void require() {
                client(
                    c -> c
                        .sslMode(SSLMode.REQUIRE)
                        .sslRootCert(SERVER.getServerCrt())
                        .sslKey(SERVER.getClientKey())
                        .sslCert(SERVER.getClientCrt()),
                    c -> c
                        .as(StepVerifier::create)
                        .expectNextCount(1)
                        .verifyComplete());
            }

            @Test
            void requireConnectsWithoutCertificate() {
                client(
                    c -> c
                        .sslMode(SSLMode.REQUIRE)
                        .sslKey(SERVER.getClientKey())
                        .sslCert(SERVER.getClientCrt()),
                    c -> c
                        .as(StepVerifier::create)
                        .expectNextCount(1)
                        .verifyComplete());
            }

            @Test
            void requireFailed() {
                client(
                    c -> c
                        .sslMode(SSLMode.REQUIRE)
                        .username(SERVER.getUsername())
                        .password(SERVER.getPassword())
                    ,
                    c -> c
                        .as(StepVerifier::create)
                        .verifyError(R2dbcPermissionDeniedException.class));
            }

            @Test
            void verifyCa() {
                client(
                    c -> c.sslRootCert(SERVER.getServerCrt())
                        .sslKey(SERVER.getClientKey())
                        .sslCert(SERVER.getClientCrt())
                        .sslMode(SSLMode.VERIFY_CA),
                    c -> c
                        .as(StepVerifier::create)
                        .expectNextCount(1)
                        .verifyComplete());
            }

            @Test
            void verifyCaWithCustomizer() {
                client(
                    c -> c.sslContextBuilderCustomizer(sslContextBuilder -> {
                        return sslContextBuilder.trustManager(new File(SERVER.getServerCrt()))
                            .keyManager(new File(SERVER.getClientCrt()), new File(SERVER.getClientKey()));
                    })
                        .sslMode(SSLMode.VERIFY_CA),
                    c -> c
                        .as(StepVerifier::create)
                        .expectNextCount(1)
                        .verifyComplete());
            }

            @Test
            void verifyCaFailedWithWrongRootCert() {
                client(
                    c -> c
                        .sslRootCert(SERVER.getClientCrt())
                        .sslMode(SSLMode.VERIFY_CA),
                    c -> c
                        .as(StepVerifier::create)
                        .verifyError(R2dbcNonTransientResourceException.class));
            }

            @Test
            void verifyCaFailedWithoutSsl() {
                client(
                    c -> c
                        .sslMode(SSLMode.VERIFY_CA)
                        .sslRootCert(SERVER.getServerCrt())
                        .sslKey(SERVER.getClientKey())
                        .sslCert(SERVER.getClientCrt())
                        .username(SERVER.getUsername())
                        .password(SERVER.getPassword()),
                    c -> c
                        .as(StepVerifier::create)
                        .verifyError(R2dbcPermissionDeniedException.class));
            }

            @Test
            void verifyFull() {
                client(
                    c -> c
                        .sslMode(SSLMode.VERIFY_FULL)
                        .sslRootCert(SERVER.getServerCrt())
                        .sslKey(SERVER.getClientKey())
                        .sslCert(SERVER.getClientCrt()),
                    c -> c
                        .as(StepVerifier::create)
                        .expectNextCount(1)
                        .verifyComplete());
            }

            @Test
            void verifyFullFailedWithWrongHost() {
                client(
                    c -> c.sslRootCert(SERVER.getServerCrt())
                        .sslHostnameVerifier(new FailedVerification())
                        .sslMode(SSLMode.VERIFY_FULL),
                    c -> c
                        .as(StepVerifier::create)
                        .verifyError(R2dbcPermissionDeniedException.class));
            }

            @Test
            void verifyFullFailedWithWrongRootCert() {
                client(
                    c -> c
                        .sslMode(SSLMode.VERIFY_FULL)
                        .sslRootCert(SERVER.getClientCrt())
                        .sslKey(SERVER.getClientKey())
                        .sslCert(SERVER.getClientCrt()),
                    c -> c
                        .as(StepVerifier::create)
                        .verifyError(R2dbcNonTransientResourceException.class));
            }

            @Test
            void verifyFullFailedWithoutSsl() {
                client(
                    c -> c
                        .sslMode(SSLMode.VERIFY_FULL)
                        .sslRootCert(SERVER.getServerCrt())
                        .username(SERVER.getUsername())
                        .password(SERVER.getPassword()),
                    c -> c
                        .as(StepVerifier::create)
                        .verifyError(R2dbcPermissionDeniedException.class));
            }
        }
    }


}
