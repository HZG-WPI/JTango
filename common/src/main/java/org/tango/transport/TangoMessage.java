package org.tango.transport;

import fr.esrf.TangoDs.TangoConst;

/**
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 19.02.2020
 */
public class TangoMessage<T> {
    public String action;
    public String device;
    public String target;
    public int dataType;
    public T value;

    public TangoMessage(String action, String device, String target, int dataType, T value) {
        this.action = action;
        this.device = device;
        this.target = target;
        this.dataType = dataType;
        this.value = value;
    }

    public static class Error extends TangoMessage {
        public Error(String value) {
            super("response", null, "error", TangoConst.Tango_DEV_STRING, value);
        }
    }

    public static class Ok extends TangoMessage {
        public Ok() {
            super("response", null, "ok", TangoConst.Tango_DEV_VOID, null);
        }
    }
}
