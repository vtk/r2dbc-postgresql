package io.r2dbc.postgresql.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.r2dbc.postgresql.message.backend.BackendKeyData;
import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.backend.BackendMessageDecoder;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.backend.Field;
import io.r2dbc.postgresql.message.backend.NoticeResponse;
import io.r2dbc.postgresql.message.backend.NotificationResponse;
import io.r2dbc.postgresql.message.backend.ParameterStatus;
import io.r2dbc.postgresql.message.backend.ReadyForQuery;
import io.r2dbc.postgresql.message.frontend.FrontendMessage;
import io.r2dbc.postgresql.message.frontend.Terminate;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.netty.tcp.TcpResources;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.concurrent.Queues;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.r2dbc.postgresql.client.TransactionStatus.IDLE;

public class NettyProtocolConnection extends ChannelInboundHandlerAdapter implements ProtocolConnection {

    private static final Logger logger = Loggers.getLogger(ReactorNettyProtocolConnection.class);

    private static final boolean DEBUG_ENABLED = logger.isDebugEnabled();

    private static final Supplier<NettyProtocolConnection.PostgresConnectionClosedException> UNEXPECTED = () -> new NettyProtocolConnection.PostgresConnectionClosedException("Connection " +
        "unexpectedly closed");

    private static final Supplier<NettyProtocolConnection.PostgresConnectionClosedException> EXPECTED = () -> new NettyProtocolConnection.PostgresConnectionClosedException("Connection closed");

    private final Queue<FluxSink<BackendMessage>> responseReceivers = Queues.<FluxSink<BackendMessage>>unbounded().get();

    private final DirectProcessor<NotificationResponse> notificationProcessor = DirectProcessor.create();

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    final Channel channel;

    private volatile Integer processId;

    private volatile Integer secretKey;

    private volatile TransactionStatus transactionStatus = IDLE;

    private volatile Version version = new Version("", 0);

    private NettyProtocolConnection(Channel channel) {
        this.channel = channel;
    }

    @Override
    public Disposable addNotificationListener(Consumer<NotificationResponse> consumer) {
        return this.notificationProcessor.subscribe(consumer);
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {

            this.drainError(EXPECTED);
            if (this.isClosed.compareAndSet(false, true)) {

                if (!this.isConnected() || this.processId == null) {
                    return NettyProtocolConnection.channelFuture(this.channel::close).then();
                }

                return NettyProtocolConnection.channelFuture(() -> this.channel.writeAndFlush(Terminate.INSTANCE))
                    .then(NettyProtocolConnection.channelFuture(this.channel::close)).then();
            }

            return Mono.empty();
        });
    }

    @Override
    public Flux<BackendMessage> exchange(Publisher<FrontendMessage> requests) {
        Assert.requireNonNull(requests, "requests must not be null");

        return Flux.create(sink -> {
            synchronized (this) {
                this.responseReceivers.add(sink);
            }
            requests.subscribe(new Subscriber<FrontendMessage>() {

                @Override
                public void onSubscribe(Subscription s) {
                    if (!NettyProtocolConnection.this.isConnected()) {
                        sink.error(new NettyProtocolConnection.PostgresConnectionClosedException("Cannot exchange messages because the connection is closed"));
                    }
                    s.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(FrontendMessage frontendMessage) {
                    if (DEBUG_ENABLED) {
                        logger.debug("Request:  {}", frontendMessage);
                    }
                    if (frontendMessage.requireFlush()) {
                        NettyProtocolConnection.this.channel.writeAndFlush(frontendMessage);
                    } else {
                        NettyProtocolConnection.this.channel.write(frontendMessage);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    sink.error(t);
                }

                @Override
                public void onComplete() {
                    if (!NettyProtocolConnection.this.isConnected()) {
                        sink.error(new NettyProtocolConnection.PostgresConnectionClosedException("Cannot exchange messages because the connection is closed"));
                    } else {
                        NettyProtocolConnection.this.channel.flush();
                    }
                }
            });
        });
    }

    @Override
    public ByteBufAllocator getByteBufAllocator() {
        return this.channel.alloc();
    }

    @Override
    public Optional<Integer> getProcessId() {
        return Optional.ofNullable(this.processId);
    }

    @Override
    public Optional<Integer> getSecretKey() {
        return Optional.ofNullable(this.secretKey);
    }

    @Override
    public TransactionStatus getTransactionStatus() {
        return this.transactionStatus;
    }

    @Override
    public Version getVersion() {
        return this.version;
    }

    @Override
    public boolean isConnected() {
        if (this.isClosed.get()) {
            return false;
        }
        return this.channel.isOpen();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        BackendMessage message = (BackendMessage) msg;
        if (DEBUG_ENABLED) {
            logger.debug("Response: {}", message);
        }
        if (message.getClass() == NoticeResponse.class) {
            logger.warn("Notice: {}", toString(((NoticeResponse) message).getFields()));
            return;
        }
        if (message.getClass() == BackendKeyData.class) {

            BackendKeyData backendKeyData = (BackendKeyData) message;

            this.processId = backendKeyData.getProcessId();
            this.secretKey = backendKeyData.getSecretKey();
            return;
        }
        if (message.getClass() == ErrorResponse.class) {
            logger.warn("Error: {}", toString(((ErrorResponse) message).getFields()));
        }
        if (message.getClass() == ParameterStatus.class) {
            handleParameterStatus((ParameterStatus) message);
        }

        if (message.getClass() == ReadyForQuery.class) {
            this.transactionStatus = TransactionStatus.valueOf(((ReadyForQuery) message).getTransactionStatus());
        }

        if (message.getClass() == NotificationResponse.class) {
            this.notificationProcessor.onNext((NotificationResponse) message);
            return;
        }

        if (message instanceof ReadyForQuery) {
            this.responseReceivers.poll().complete();
        } else {
            this.responseReceivers.peek().next(message);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        this.drainError(() -> new NettyProtocolConnection.PostgresConnectionException(cause));
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        handleClose();
        super.channelUnregistered(ctx);
    }

    private void handleClose() {
        if (this.isClosed.compareAndSet(false, true)) {
            this.drainError(UNEXPECTED);
        } else {
            this.drainError(EXPECTED);
        }
    }

    private void drainError(Supplier<? extends Throwable> supplier) {
        FluxSink<BackendMessage> receiver;

        while ((receiver = this.responseReceivers.poll()) != null) {
            receiver.error(supplier.get());
        }
    }

    private void handleParameterStatus(ParameterStatus message) {
        Version existingVersion = this.version;

        String versionString = existingVersion.getVersion();
        int versionNum = existingVersion.getVersionNumber();

        if (message.getName().equals("server_version_num")) {
            versionNum = Integer.parseInt(message.getValue());
        }

        if (message.getName().equals("server_version")) {
            versionString = message.getValue();

            if (versionNum == 0) {
                versionNum = Version.parseServerVersionStr(versionString);
            }
        }

        this.version = new Version(versionString, versionNum);
    }

    public static ProtocolConnectionProvider PROVIDER = (endpoint, connectTimeout, sslConfig) -> {
        Assert.requireNonNull(endpoint, "endpoint must not be null");
        return Mono.defer(() -> {
            Assert.requireNonNull(endpoint, "endpoint must not be null");

            Bootstrap b = new Bootstrap();
            if (!(endpoint instanceof InetSocketAddress)) {
                b.group(new ReactorNettyProtocolConnection.SocketLoopResources().onClient(true));
            } else {
                b.group(TcpResources.get().onClient(true));
            }
            b.channel(EpollSocketChannel.class);// todo
            if (connectTimeout != null) {
                b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(connectTimeout.toMillis()));
            }
            PostgresClientChannelInitializer postgresClientChannelInitializer = new PostgresClientChannelInitializer(sslConfig);
            b.handler(postgresClientChannelInitializer);

            Mono<Channel> connectMono = NettyProtocolConnection.channelFuture(() -> b.connect(endpoint));
            return connectMono.flatMap(postgresClientChannelInitializer.sslHandshake::thenReturn).map(channel -> {
                NettyProtocolConnection nettyProtocolConnection = new NettyProtocolConnection(channel);
                channel.pipeline().addLast(nettyProtocolConnection);
                return (nettyProtocolConnection);
            });
        });
    };

    private static Mono<Channel> channelFuture(Supplier<ChannelFuture> supplier) {
        return Mono.create(sink -> {
            ChannelFuture connect = supplier.get();
            connect.addListener(future -> {
                if (future.isSuccess()) {
                    sink.success(connect.channel());
                } else if (future.isCancelled()) {
                    sink.onCancel(() -> {
                    });
                } else {
                    sink.error(future.cause());
                }
            });
        });
    }

    private static String toString(List<Field> fields) {

        StringJoiner joiner = new StringJoiner(", ");
        for (Field field : fields) {
            joiner.add(field.getType().name() + "=" + field.getValue());
        }

        return joiner.toString();
    }

    static class PostgresConnectionException extends R2dbcNonTransientResourceException {

        public PostgresConnectionException(Throwable cause) {
            super(cause);
        }
    }

    static class PostgresConnectionClosedException extends R2dbcNonTransientResourceException {

        public PostgresConnectionClosedException(String reason) {
            super(reason);
        }
    }

    private static class PostgresClientChannelInitializer extends ChannelInitializer<SocketChannel> {

        private final SSLConfig sslConfig;

        private final SSLSessionHandlerAdapter sslSessionHandlerAdapter;

        private final Mono<Void> sslHandshake;

        private PostgresClientChannelInitializer(SSLConfig sslConfig) {
            this.sslConfig = sslConfig;
            if (sslConfig.getSslMode().startSsl()) {
                this.sslSessionHandlerAdapter = new SSLSessionHandlerAdapter(ByteBufAllocator.DEFAULT, sslConfig); // TODO
                this.sslHandshake = this.sslSessionHandlerAdapter.getHandshake();
            } else {
                this.sslSessionHandlerAdapter = null;
                this.sslHandshake = Mono.empty();
            }
        }

        @Override
        protected void initChannel(SocketChannel ch) {
            ChannelPipeline pipeline = ch.pipeline();
            if (logger.isTraceEnabled()) {
                pipeline.addFirst(LoggingHandler.class.getSimpleName(), new LoggingHandler(NettyProtocolConnection.class, LogLevel.TRACE));
            }
            if (this.sslConfig.getSslMode().startSsl()) {
                pipeline.addFirst(this.sslSessionHandlerAdapter);
                if (logger.isTraceEnabled()) {
                    pipeline.addFirst(LoggingHandler.class.getSimpleName() + ".SSL", new LoggingHandler(NettyProtocolConnection.class + ".SSL", LogLevel.TRACE));
                }
            }
            pipeline.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE - 5, 1, 4, -4, 0));
            pipeline.addLast(new ByteToMessageDecoder() {

                @Override
                protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
                    out.add(BackendMessageDecoder.decode(in));
                }
            });
            pipeline.addLast(new MessageToByteEncoder<FrontendMessage>() {

                @Override
                protected void encode(ChannelHandlerContext ctx, FrontendMessage msg, ByteBuf out) {
                    msg.encode(out);
                }
            });
        }
    }
}
