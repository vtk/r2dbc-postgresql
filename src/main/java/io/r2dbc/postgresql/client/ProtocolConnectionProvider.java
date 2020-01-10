package io.r2dbc.postgresql.client;

import reactor.core.publisher.Mono;
import reactor.netty.resources.ConnectionProvider;
import reactor.util.annotation.Nullable;

import java.net.SocketAddress;
import java.time.Duration;

public interface ProtocolConnectionProvider {

    Mono<ProtocolConnection> connect(SocketAddress endpoint, @Nullable Duration connectTimeout, SSLConfig sslConfig);

    ProtocolConnectionProvider reactorNettyClientProvider = (endpoint, connectTimeout, sslConfig)
        -> ReactorNettyProtocolConnection.connect(ConnectionProvider.newConnection(), endpoint, connectTimeout, sslConfig).cast(ProtocolConnection.class);

    ProtocolConnectionProvider nettyClientProvider = NettyProtocolConnection.PROVIDER;
}
