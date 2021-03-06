/*
 * Copyright 2017-2020 the original author or authors.
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

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.TestClient;
import io.r2dbc.postgresql.codec.MockCodecs;
import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.backend.DataRow;
import io.r2dbc.postgresql.message.backend.EmptyQueryResponse;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.backend.RowDescription;
import io.r2dbc.postgresql.message.frontend.Query;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.util.Collections;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

final class SimpleQueryPostgresqlStatementTest {

    @Test
    void bind() {
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> new SimpleQueryPostgresqlStatement(MockContext.empty(), "test-query").bind(null, null))
            .withMessage("Binding parameters is not supported for the statement 'test-query'");
    }

    @Test
    void bindIndex() {
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> new SimpleQueryPostgresqlStatement(MockContext.empty(), "test-query").bind(0, null))
            .withMessage("Binding parameters is not supported for the statement 'test-query'");
    }


    @Test
    void bindNull() {
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> new SimpleQueryPostgresqlStatement(MockContext.empty(), "test-query").bindNull(null, null))
            .withMessage("Binding parameters is not supported for the statement 'test-query'");
    }

    @Test
    void bindNullIndex() {
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> new SimpleQueryPostgresqlStatement(MockContext.empty(), "test-query").bindNull(0, null))
            .withMessage("Binding parameters is not supported for the statement 'test-query'");
    }


    @Test
    void constructorNoContext() {
        assertThatIllegalArgumentException().isThrownBy(() -> new SimpleQueryPostgresqlStatement(null, "test-query"))
            .withMessage("context must not be null");
    }

    @Test
    void constructorNoSql() {
        assertThatIllegalArgumentException().isThrownBy(() -> new SimpleQueryPostgresqlStatement(MockContext.empty(), null))
            .withMessage("sql must not be null");
    }

    @Test
    void executeCommandCompleteMap() {
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query")).thenRespond(new CommandComplete("test", null, 1))
            .build();

        new SimpleQueryPostgresqlStatement(MockContext.builder().client(client).build(), "test-query")
            .execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row))
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void executeCommandCompleteRowsUpdated() {
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query")).thenRespond(new CommandComplete("test", null, 1))
            .build();

        new SimpleQueryPostgresqlStatement(MockContext.builder().client(client).build(), "test-query")
            .execute()
            .flatMap(PostgresqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
    }

    @Test
    void executeEmptyQueryResponseRows() {
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query")).thenRespond(EmptyQueryResponse.INSTANCE)
            .build();

        new SimpleQueryPostgresqlStatement(MockContext.builder().client(client).build(), "test-query")
            .execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row))
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void executeEmptyQueryResponseRowsUpdated() {
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query")).thenRespond(EmptyQueryResponse.INSTANCE)
            .build();

        new SimpleQueryPostgresqlStatement(MockContext.builder().client(client).build(), "test-query")
            .execute()
            .flatMap(PostgresqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void executeErrorResponseRows() {
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query")).thenRespond(new ErrorResponse(Collections.emptyList()))
            .build();

        new SimpleQueryPostgresqlStatement(MockContext.builder().client(client).build(), "test-query")
            .execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row))
            .as(StepVerifier::create)
            .verifyError(R2dbcNonTransientResourceException.class);
    }

    @Test
    void executeErrorResponseRowsUpdated() {
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query")).thenRespond(new ErrorResponse(Collections.emptyList()))
            .build();

        new SimpleQueryPostgresqlStatement(MockContext.builder().client(client).build(), "test-query")
            .execute()
            .flatMap(PostgresqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .verifyError(R2dbcNonTransientResourceException.class);
    }

    @Test
    void executeErrorResponse() {
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query")).thenRespond(new ErrorResponse(Collections.emptyList()))
            .build();

        new SimpleQueryPostgresqlStatement(MockContext.builder().client(client).build(), "test-query")
            .execute()
            .flatMap(PostgresqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .verifyError(R2dbcNonTransientResourceException.class);
    }

    @Test
    void executeRowDescriptionRows() {
        RowDescription.Field field = new RowDescription.Field((short) 100, 200, 300, (short) 400, FORMAT_TEXT, "test-name", 500);
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query"))
            .thenRespond(
                new RowDescription(Collections.singletonList(field)),
                new DataRow(TEST.buffer(4).writeInt(100)),
                new CommandComplete("test", null, null))
            .build();

        MockCodecs codecs = MockCodecs.builder()
            .preferredType(200, FORMAT_TEXT, String.class)
            .build();

        ConnectionContext context = MockContext.builder().client(client).codecs(codecs).build();

        new SimpleQueryPostgresqlStatement(context, "test-query")
            .execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row))
            .as(StepVerifier::create)
            .expectNext(new PostgresqlRow(context, Collections.singletonList(field),
                new ByteBuf[]{TEST.buffer(4).writeInt(100)}))
            .verifyComplete();
    }

    @Test
    void executeRowDescriptionRowsUpdated() {
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query"))
            .thenRespond(
                new RowDescription(Collections.singletonList(new RowDescription.Field((short) 100, 200, 300, (short) 400, FORMAT_TEXT, "test-name", 500))),
                new DataRow(TEST.buffer(4).writeInt(100)),
                new CommandComplete("test", null, null))
            .build();

        new SimpleQueryPostgresqlStatement(MockContext.builder().client(client).build(), "test-query")
            .execute()
            .flatMap(PostgresqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void returnGeneratedValues() {
        Client client = TestClient.builder()
            .expectRequest(new Query("INSERT test-query RETURNING *")).thenRespond(new CommandComplete("test", null, 1))
            .build();

        new SimpleQueryPostgresqlStatement(MockContext.builder().client(client).build(), "INSERT test-query")
            .returnGeneratedValues()
            .execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row))
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void returnGeneratedValuesHasReturningClause() {
        assertThatIllegalStateException().isThrownBy(() -> new SimpleQueryPostgresqlStatement(MockContext.empty(), "RETURNING").returnGeneratedValues())
            .withMessage("Statement already includes RETURNING clause");
    }

    @Test
    void returnGeneratedValuesUnsupportedCommand() {
        assertThatIllegalStateException().isThrownBy(() -> new SimpleQueryPostgresqlStatement(MockContext.empty(), "SELECT").returnGeneratedValues())
            .withMessage("Statement is not a DELETE, INSERT, or UPDATE command");
    }

    @Test
    void supportsMultilineParameterSymbol() {
        assertThat(SimpleQueryPostgresqlStatement.supports("test-query-0\ntest-query-$1")).isFalse();
    }

    @Test
    void supportsNoSql() {
        assertThatIllegalArgumentException().isThrownBy(() -> SimpleQueryPostgresqlStatement.supports(null))
            .withMessage("sql must not be null");
    }

    @Test
    void supportsParameterSymbol() {
        assertThat(SimpleQueryPostgresqlStatement.supports("test-query-$1")).isFalse();
    }

    @Test
    void supportsQueryEmpty() {
        assertThat(SimpleQueryPostgresqlStatement.supports(" ")).isTrue();
    }

    @Test
    void supportsSemicolon() {
        assertThat(SimpleQueryPostgresqlStatement.supports("test-query-1 ; test-query-2")).isTrue();
    }

    @Test
    void supportsSimple() {
        assertThat(SimpleQueryPostgresqlStatement.supports("test-query")).isTrue();
    }

}
