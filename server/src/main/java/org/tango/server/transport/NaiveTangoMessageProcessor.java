package org.tango.server.transport;

import fr.esrf.Tango.AttrDataFormat;
import fr.esrf.Tango.AttributeValue;
import fr.esrf.Tango.DevFailed;
import org.omg.CORBA.Any;
import org.tango.server.admin.AdminDevice;
import org.tango.server.idl.CleverAnyAttribute;
import org.tango.server.idl.TangoIDLAttributeUtil;
import org.tango.server.servant.DeviceImpl;
import org.tango.transport.TangoMessage;

import java.util.Arrays;

/**
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 21.02.2020
 */
public class NaiveTangoMessageProcessor implements TangoMessageProcessor {
    @Override
    public TangoMessage process(TangoMessage input, AdminDevice locator) {
        DeviceImpl device = locator.getDeviceImpl(input.device);

        switch (input.action) {
            case "read":
                return processRead(input, device);
            case "write":
                return processWrite(input, device);
            case "exec":
            default:
                return new TangoMessage.Error("Unsupported message action - " + input.action);
        }
    }

    private TangoMessage processRead(TangoMessage message, DeviceImpl device) {
        try {
            Any any = Arrays.stream(device.read_attributes(new String[]{message.target})).map(attributeValue -> attributeValue.value).findFirst().get();

            Object result = CleverAnyAttribute.get(any, message.dataType, AttrDataFormat.SCALAR);
            return new TangoMessage("response", message.device, message.target, message.dataType, result);
        } catch (DevFailed devFailed) {
            return new TangoMessage.Error(devFailed.errors[0].reason);
        }
    }

    private TangoMessage processWrite(TangoMessage message, DeviceImpl device) {
        try {
            device.write_attributes(
                    new AttributeValue[]{
                            TangoIDLAttributeUtil.toAttributeValue(
                                    device.getAttributeImpl(message.target).get(),
                                    new org.tango.server.attribute.AttributeValue(message.value))});
            return new TangoMessage.Ok();
        } catch (DevFailed devFailed) {
            return new TangoMessage.Error(devFailed.errors[0].reason);
        }
    }

}
