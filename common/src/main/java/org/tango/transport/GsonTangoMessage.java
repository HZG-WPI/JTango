package org.tango.transport;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 21.02.2020
 */
public class GsonTangoMessage implements TangoMessageMarshaller, TangoMessageUnmarshaller {
    private final Gson gson = new GsonBuilder()
            .serializeNulls()
            .create();


    @Override
    public byte[] marshal(TangoMessage tangoMessage) {
        return gson.toJson(tangoMessage).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public TangoMessage unmarshal(InputStream stream) {
        return gson.fromJson(new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8)), TangoMessage.class);
    }

    @Override
    public TangoMessage unmarshal(byte[] stream) {
        return unmarshal(new String(stream, StandardCharsets.UTF_8));
    }

    @Override
    public TangoMessage unmarshal(String stream) {
        return gson.fromJson(stream, TangoMessage.class);
    }
}
