package org.tango.transport;

import java.io.InputStream;

/**
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 21.02.2020
 */
public interface TangoMessageUnmarshaller {
    TangoMessage unmarshal(InputStream stream);

    TangoMessage unmarshal(byte[] stream);

    TangoMessage unmarshal(String stream);
}
