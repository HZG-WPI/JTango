package org.tango.transport;

import java.io.IOException;

/**
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 21.02.2020
 */
public class DefaultTransport implements Transport {
    @Override
    public boolean isConnected() {
        return false;
    }

    @Override
    public void connect(String endpoint) throws IOException {
        throw new UnsupportedOperationException("This method is not supported in " + this.getClass());
    }

    @Override
    public void disconnect(String endpoint) throws IOException {
        throw new UnsupportedOperationException("This method is not supported in " + this.getClass());
    }

    @Override
    public byte[] send(byte[] data) throws IOException {
        throw new UnsupportedOperationException("This method is not supported in " + this.getClass());
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("This method is not supported in " + this.getClass());
    }
}
