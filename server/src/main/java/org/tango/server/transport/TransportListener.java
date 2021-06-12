package org.tango.server.transport;

import java.util.List;

/**
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 21.02.2020
 */
public interface TransportListener {
    void bind();

    void listen();

    String getName();

    List<String> endpoints();
}
