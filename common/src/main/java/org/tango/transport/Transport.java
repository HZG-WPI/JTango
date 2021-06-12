package org.tango.transport;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 19.02.2020
 */
public interface Transport extends Closeable {
    boolean isConnected();

    void connect(String endpoint) throws IOException;

    void disconnect(String endpoint) throws IOException;

    byte[] send(byte[] data) throws IOException;
}
