package org.tango.transport;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;

/**
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 19.02.2020
 */
//@ThreadSafe
public class ZmqTransport implements Transport {
    private final ZContext zcontext = new ZContext();
    private ZMQ.Socket socket;

    @Override
    public boolean isConnected() {
        return socket != null;
    }

    @Override
    public void connect(String endpoint) {
        ZMQ.Socket socket = zcontext.createSocket(ZMQ.REQ);

        socket.connect(endpoint);

        this.socket = socket;
    }

    @Override
    public void disconnect(String endpoint) {
        zcontext.getSockets()
                .forEach(socket -> socket.disconnect(endpoint));
    }

    @Override
    public byte[] send(byte[] data) {
        socket.send(data);

        return socket.recv();
    }

    @Override
    public void close() {
        zcontext.close();
    }
}
