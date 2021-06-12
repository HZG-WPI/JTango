package org.tango.server.transport;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * This class maintains ZMQ REQ/REP required data
 *
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 18.02.2020
 */
public class TransportManager {
    private final Map<String, TransportListener> transports = Maps.newHashMap();

    public void registerTransport(TransportListener transportListener) {
        transports.put(transportListener.getName(), transportListener);
    }

    public List<String> getEndpoints(String transport) {
        return transports.get(transport).endpoints();
    }

    public List<String> getTransports() {
        return Lists.newArrayList(transports.keySet());
    }

    public void bind() {
        transports.values().forEach(TransportListener::bind);
    }

    public void listen() {
        transports.values().forEach(TransportListener::listen);
    }
}
