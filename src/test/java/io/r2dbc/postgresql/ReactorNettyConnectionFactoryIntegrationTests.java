package io.r2dbc.postgresql;

import io.r2dbc.postgresql.client.ProtocolConnectionProvider;

public class ReactorNettyConnectionFactoryIntegrationTests extends AbstractConnectionFactoryIntegrationTests {

    @Override
    ProtocolConnectionProvider provider() {
        return ProtocolConnectionProvider.reactorNettyClientProvider;
    }
}