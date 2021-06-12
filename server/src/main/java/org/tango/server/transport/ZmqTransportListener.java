package org.tango.server.transport;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tango.network.NetworkUtils;
import org.tango.server.admin.AdminDevice;
import org.tango.transport.GsonTangoMessage;
import org.tango.transport.TangoMessage;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 19.02.2020
 */
public class ZmqTransportListener implements TransportListener, Runnable {
    private final GsonTangoMessage marshaller = new GsonTangoMessage();
    private final TangoMessageProcessor tangoMessageProcessor = new NaiveTangoMessageProcessor();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("ZmqTransportListener-%d")
                    .build()
    );
    private final ZContext ctx = new ZContext();


    private final Logger logger = LoggerFactory.getLogger(ZmqTransportListener.class);
    private String port;
    private final ZMQ.Socket socket;
    private final AdminDevice admin;

    public ZmqTransportListener(AdminDevice admin) {
        this.admin = admin;
        this.socket = createZMQSocket(ZMQ.REP);
    }

    @Override
    public void bind() {
        port = String.valueOf(socket.bindToRandomPort("tcp://*"));
    }

    @Override
    public String getName() {
        return "zmq";
    }

    @Override
    public List<String> endpoints() {
        List<String> ip4Addresses = NetworkUtils.getInstance().getIp4Addresses();

        return ip4Addresses.stream()
                .map(s -> "tcp://" + s + ":" + port)
                .collect(Collectors.toList());
    }

    public void listen() {
        executorService.submit(this);
    }


    @Override
    public void run() {
        logger.debug("Starting ZmqTransportListener");
        while (!Thread.currentThread().isInterrupted()) {
            byte[] data = socket.recv();

            TangoMessage message = marshaller.unmarshal(new String(data, StandardCharsets.UTF_8));

            socket.send(
                    marshaller.marshal(
                            tangoMessageProcessor.process(message, admin)));

        }
    }

    private ZMQ.Socket createZMQSocket(int type) {
        ZMQ.Socket socket = ctx.createSocket(type);
//        socket.setLinger(0);
        return socket;
    }
}
