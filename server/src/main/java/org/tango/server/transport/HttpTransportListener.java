package org.tango.server.transport;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.tango.network.NetworkUtils;
import org.tango.server.admin.AdminDevice;
import org.tango.transport.GsonTangoMessage;
import org.tango.transport.TangoMessage;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 20.02.2020
 */
public class HttpTransportListener implements TransportListener {
    private final GsonTangoMessage marshaller = new GsonTangoMessage();
    private final TangoMessageProcessor tangoMessageProcessor = new NaiveTangoMessageProcessor();
    private final AdminDevice adminDevice;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("HttpTransportListener-%d")
                    .build()
    );
    private Undertow server;


    public HttpTransportListener(AdminDevice adminDevice) {
        this.adminDevice = adminDevice;
    }

    @Override
    public void bind() {
        int port = NetworkUtils.getInstance().getRandomPort();

        Undertow.Builder builder = Undertow.builder()
                .setIoThreads(200)
                .setServerOption(UndertowOptions.ENABLE_HTTP2, true)
                .setHandler(new HttpHandler() {
                                @Override
                                public void handleRequest(HttpServerExchange exchange) {
                                    if (exchange.isInIoThread()) {
                                        exchange.dispatch(this);
                                        return;
                                    }

                                    exchange.startBlocking();

                                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");

                                    TangoMessage message = marshaller.unmarshal(exchange.getInputStream());

                                    message = tangoMessageProcessor.process(message, adminDevice);

                                    exchange.getResponseSender().send(ByteBuffer.wrap(marshaller.marshal(message)));
                                }
                            }
                );

        NetworkUtils.getInstance().getIp4Addresses().forEach(s -> builder.addHttpListener(port, s));

        this.server = builder.build();
    }

    @Override
    public void listen() {
        executorService.submit(() -> server.start());
    }

    @Override
    public String getName() {
        return "http";
    }

    @Override
    public List<String> endpoints() {
        return server.getListenerInfo().stream()
                .map(listenerInfo -> listenerInfo.getProtcol() + ":/" + listenerInfo.getAddress().toString())
                .collect(Collectors.toList());
    }
}
