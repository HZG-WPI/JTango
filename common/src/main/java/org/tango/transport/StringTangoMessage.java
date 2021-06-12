package org.tango.transport;

import fr.esrf.TangoDs.TangoConst;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 21.02.2020
 */
public class StringTangoMessage implements TangoMessageMarshaller, TangoMessageUnmarshaller {
    public static final String PATTERN_STR = "(read|write|exec|response);([\\w\\W]+)/([\\w\\W]+);(\\d+):([\\w.]+)";
    public static final Pattern PATTERN = Pattern.compile(PATTERN_STR);

    @Override
    public byte[] marshal(TangoMessage tangoMessage) {
        return toString(tangoMessage).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public TangoMessage unmarshal(InputStream stream) {
        BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        return fromString(br.lines().collect(Collectors.joining(System.lineSeparator())));
    }

    @Override
    public TangoMessage unmarshal(byte[] stream) {
        return unmarshal(new ByteArrayInputStream(stream));
    }

    @Override
    public TangoMessage unmarshal(String stream) {
        return fromString(stream);
    }

    private String toString(TangoMessage message) {
        return String.format(
                "%s;%s/%s;%d:%s", message.action, message.device, message.target, message.dataType, String.valueOf(message.value)
        );
    }

    private TangoMessage fromString(String str) {
        Matcher matcher = PATTERN.matcher(str);

        if (matcher.matches()) {
            int dataType = Integer.parseInt(matcher.group(4));
            return new TangoMessage(matcher.group(1), matcher.group(2), matcher.group(3), dataType, parseValue(dataType, matcher.group(5)));
        } else
            throw new IllegalArgumentException("Unrecognized message: " + str);
    }

    private Object parseValue(int dataType, String value) {
        switch (dataType) {
            case TangoConst
                    .Tango_DEV_DOUBLE:
                return Double.valueOf(value);
            default:
                return value;
        }
    }
}
