package org.tango.server.transport;

import org.tango.server.admin.AdminDevice;
import org.tango.transport.TangoMessage;

/**
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 21.02.2020
 */
public interface TangoMessageProcessor {
    TangoMessage process(TangoMessage input, AdminDevice locator);
}
