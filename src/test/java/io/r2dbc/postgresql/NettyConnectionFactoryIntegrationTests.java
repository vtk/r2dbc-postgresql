package io.r2dbc.postgresql;

import io.r2dbc.postgresql.client.ProtocolConnectionProvider;

class NettyConnectionFactoryIntegrationTests extends AbstractConnectionFactoryIntegrationTests {

    @Override
    ProtocolConnectionProvider provider() {
        return ProtocolConnectionProvider.nettyClientProvider;
    }
}
