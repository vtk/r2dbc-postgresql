package io.r2dbc.postgresql.client;

import io.netty.channel.Channel;
import org.junit.jupiter.api.Disabled;

final class NettyProtocolConnectionIntegrationTests extends AbstractProtocolConnectionTest {

    @Override
    protected ProtocolConnectionProvider provider() {
        return NettyProtocolConnection.PROVIDER;
    }

    @Override
    protected Channel getChannel(ProtocolConnection connection) {
        return ((NettyProtocolConnection) connection).channel;
    }

    @Override
    @Disabled("Kind of hard to implement this one")
    void shouldCancelExchangeOnCloseInFlight() throws Exception {
        super.shouldCancelExchangeOnCloseInFlight();
    }

    @Disabled("Kind of hard to implement this one")
    @Override
    void shouldCancelExchangeOnCloseFirstMessage() throws Exception {
        super.shouldCancelExchangeOnCloseFirstMessage();
    }
}
