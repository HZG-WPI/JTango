package org.tango.transport;

/**
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 21.02.2020
 */
public interface TangoMessageMarshaller {
    byte[] marshal(TangoMessage tangoMessage);
}
