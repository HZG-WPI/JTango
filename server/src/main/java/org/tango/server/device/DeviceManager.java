/**
 * Copyright (C) :     2012
 * <p>
 * Synchrotron Soleil
 * L'Orme des merisiers
 * Saint Aubin
 * BP48
 * 91192 GIF-SUR-YVETTE CEDEX
 * <p>
 * This file is part of Tango.
 * <p>
 * Tango is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * Tango is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Lesser General Public License
 * along with Tango.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.tango.server.device;

import fr.esrf.Tango.ClntIdent;
import fr.esrf.Tango.DevFailed;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.tango.DeviceState;
import org.tango.orb.ServerRequestInterceptor;
import org.tango.server.annotation.DeviceManagement;
import org.tango.server.attribute.AttributeImpl;
import org.tango.server.attribute.AttributePropertiesImpl;
import org.tango.server.attribute.AttributeValue;
import org.tango.server.command.CommandImpl;
import org.tango.server.events.EventType;
import org.tango.server.events.ZmqEventManager;
import org.tango.server.pipe.PipeImpl;
import org.tango.server.pipe.PipeValue;
import org.tango.server.servant.AttributeGetterSetter;
import org.tango.server.servant.DeviceImpl;
import org.tango.utils.ClientIDUtil;
import org.tango.utils.DevFailedUtils;

/**
 * Global info and tool for a device. Injected with {@link DeviceManagement}
 *
 * @author ABEILLE
 */
public class DeviceManager {
    private final DeviceImpl device;
    private final String name;
    private final String className;
    private final String adminName;

    /**
     * Ctr
     *
     * @param device
     */
    public DeviceManager(final DeviceImpl device) {
        this.device = device;
        name = device.getName();
        className = device.getClassName();
        adminName = device.getAdminDeviceName();
    }

    /**
     * @return name of the device
     */
    public String getName() {
        return name;
    }

    /**
     * @return class of the device, as defined in tango db
     */
    public String getClassName() {
        return className;
    }

    /**
     * @return admin device name
     */
    public String getAdminName() {
        return adminName;
    }

    /**
     * Get an attribute's properties
     *
     * @param attributeName the attribute name
     * @return its properties
     * @throws DevFailed
     */
    public AttributePropertiesImpl getAttributeProperties(final String attributeName) throws DevFailed {
        final AttributeImpl attr = AttributeGetterSetter.getAttribute(attributeName, device.getAttributeList());
        return attr.getProperties();
    }

    /**
     * Configure an attribute's properties
     *
     * @param attributeName the attribute name
     * @param properties    its properties
     * @throws DevFailed
     */
    public void setAttributeProperties(final String attributeName, final AttributePropertiesImpl properties)
            throws DevFailed {
        final AttributeImpl attr = AttributeGetterSetter.getAttribute(attributeName, device.getAttributeList());
        attr.setProperties(properties);
    }

    /**
     * Remove an attribute's properties
     *
     * @param attributeName the attribute name
     * @throws DevFailed
     */
    public void removeAttributeProperties(final String attributeName) throws DevFailed {
        final AttributeImpl attr = AttributeGetterSetter.getAttribute(attributeName, device.getAttributeList());
        attr.removeProperties();
    }

    /**
     * Check if an attribute or an command is polled
     *
     * @param polledObject The name of the polled object (attribute or command)
     * @return true if polled
     * @throws DevFailed
     */
    public boolean isPolled(final String polledObject) throws DevFailed {
        try {
            return AttributeGetterSetter.getAttribute(polledObject, device.getAttributeList()).isPolled();
        } catch (final DevFailed e) {
            return device.getCommand(polledObject).isPolled();
        }
    }

    /**
     * Get polling period of an attribute or a command
     *
     * @param polledObject The name of the polled object (attribute or command)
     * @return The polling period
     * @throws DevFailed
     */
    public int getPollingPeriod(final String polledObject) throws DevFailed {
        try {
            return AttributeGetterSetter.getAttribute(polledObject, device.getAttributeList()).getPollingPeriod();
        } catch (final DevFailed e) {
            return device.getCommand(polledObject).getPollingPeriod();
        }
    }

    /**
     * Configure polling of an attribute or a command and start it
     *
     * @param polledObject  The name of the polled object (attribute or command)
     * @param pollingPeriod The polling period
     * @throws DevFailed
     */
    public void startPolling(final String polledObject, final int pollingPeriod) throws DevFailed {
        try {
            final AttributeImpl attr = AttributeGetterSetter.getAttribute(polledObject, device.getAttributeList());
            attr.configurePolling(pollingPeriod);
            device.startPolling(attr);
        } catch (final DevFailed e) {
            if (polledObject.equalsIgnoreCase(DeviceImpl.STATE_NAME)
                    || polledObject.equalsIgnoreCase(DeviceImpl.STATUS_NAME)) {
                final CommandImpl cmd = device.getCommand(polledObject);
                cmd.configurePolling(pollingPeriod);
                device.startPolling(cmd);
            } else {
                throw e;
            }
        }
    }

    /**
     * Start polling of an attribute or a command
     *
     * @param polledObject The name of the polled object (attribute or command)
     * @throws DevFailed
     */
    public void startPolling(final String polledObject) throws DevFailed {
        try {
            final AttributeImpl attr = AttributeGetterSetter.getAttribute(polledObject, device.getAttributeList());
            device.startPolling(attr);
        } catch (final DevFailed e) {
            if (polledObject.equalsIgnoreCase(DeviceImpl.STATE_NAME)
                    || polledObject.equalsIgnoreCase(DeviceImpl.STATUS_NAME)) {
                final CommandImpl cmd = device.getCommand(polledObject);
                device.startPolling(cmd);
            } else {
                throw e;
            }
        }
    }

    public void stopPolling(final String polledObject) throws DevFailed {
        try {
            device.removeAttributePolling(polledObject);
        } catch (final DevFailed e) {
            if (polledObject.equalsIgnoreCase(DeviceImpl.STATE_NAME)
                    || polledObject.equalsIgnoreCase(DeviceImpl.STATUS_NAME)) {
                device.removeCommandPolling(polledObject);
            } else {
                throw e;
            }
        }
    }

    /**
     * Update polling cache. Works only if polling period is zero.
     *
     * @param polledObject The name of the polled object (attribute or command)
     * @throws DevFailed
     */
    public void triggerPolling(final String polledObject) throws DevFailed {
        device.triggerPolling(polledObject);
    }

    /**
     * Push an event if some client had register it. The method will perform a read on the requested attribute before
     * sending the event
     *
     * @param attributeName The attribute name
     * @param eventType     The type of event to fire
     * @throws DevFailed
     */
    public void pushEvent(final String attributeName, final EventType eventType) throws DevFailed {
        switch (eventType) {
            case CHANGE_EVENT:
            case ARCHIVE_EVENT:
            case USER_EVENT:
                // get attribute value
                final AttributeImpl attribute = AttributeGetterSetter.getAttribute(attributeName,
                        device.getAttributeList());
                attribute.lock();
                try {
                    attribute.updateValue();
                    // push the event
                    ZmqEventManager.getInstance().pushAttributeValueEvent(name, attributeName, eventType);
                } catch (final DevFailed e) {
                    ZmqEventManager.getInstance().pushAttributeErrorEvent(name, attributeName, e);
                } finally {
                    attribute.unlock();
                }
                break;
            default:
                throw DevFailedUtils.newDevFailed("Only USER, ARCHIVE or CHANGE event can be send");
        }
    }

    /**
     * Push an event if some client had register it.
     *
     * @param attributeName The attribute name
     * @param eventType     The type of event to fire
     * @throws DevFailed
     */
    public void pushEvent(final String attributeName, final AttributeValue value, final EventType eventType)
            throws DevFailed {
        switch (eventType) {
            case CHANGE_EVENT:
            case ARCHIVE_EVENT:
            case USER_EVENT:
                // set attribute value
                final AttributeImpl attribute = AttributeGetterSetter.getAttribute(attributeName,
                        device.getAttributeList());
                attribute.lock();
                try {
                    // convert to the State type use on API side
                    if (value.getValue() instanceof  DeviceState) {
                        DeviceState state = (DeviceState) value.getValue();
                        value.setValue(state.getDevState());
                    }
                    attribute.updateValue(value);
                    // push the event
                    ZmqEventManager.getInstance().pushAttributeValueEvent(name, attributeName, eventType);
                } catch (final DevFailed e) {
                    ZmqEventManager.getInstance().pushAttributeErrorEvent(name, attributeName, e);
                } finally {
                    attribute.unlock();
                }
                break;
            default:
                throw DevFailedUtils.newDevFailed("Only USER, ARCHIVE or CHANGE event can be send");
        }
    }

    /**
     * Push a DATA_READY event if some client had registered it
     *
     * @param attributeName The attribute name
     * @param counter
     * @throws DevFailed
     */
    public void pushDataReadyEvent(final String attributeName, final int counter) throws DevFailed {
        ZmqEventManager.getInstance().pushAttributeDataReadyEvent(name, attributeName, counter);
    }

    /**
     * Push a PIPE EVENT event if some client had registered it
     *
     * @param pipeName The pipe name
     * @param blob     The pipe data
     * @throws DevFailed
     */
    public void pushPipeEvent(final String pipeName, final PipeValue blob) throws DevFailed {
        // set attribute value
        final PipeImpl pipe = DeviceImpl.getPipe(pipeName, device.getPipeList());
        try {
            pipe.updateValue(blob);
            // push the event
            ZmqEventManager.getInstance().pushPipeEvent(name, pipeName, blob);
        } catch (final DevFailed e) {
            ZmqEventManager.getInstance().pushPipeEvent(name, pipeName, e);
        }
    }

    /**
     * Get the client identity of the current request. WARNING: works only if the client is of version IDL4 and for
     * these requests: read_attribute(s), write_attribute(s), write_read_attribute(s), command_inout.
     *
     * @return the client identity
     */
    @Deprecated
    public ClntIdent getClientIdentity() {
        return ClientIDUtil.copyClntIdent(device.getClientIdentity());
    }

    /**
     * Get the client host name of the current request
     *
     * @return
     */
    @Deprecated
    public String getClientHostName() {
        return ServerRequestInterceptor.getInstance().getClientHostName();
    }

    public boolean hasEventSubsriber() {
        return ZmqEventManager.getInstance().hasSubscriber(name);
    }

    @Override
    public String toString() {
        final ReflectionToStringBuilder reflectionToStringBuilder = new ReflectionToStringBuilder(this,
                ToStringStyle.SHORT_PREFIX_STYLE);
        reflectionToStringBuilder.setExcludeFieldNames(new String[]{"device"});
        return reflectionToStringBuilder.toString();
    }

    public DeviceImpl getDevice() {
        return device;
    }

    public void pushStateChangeEvent() {
        try {
            pushEvent("State", new AttributeValue(device.getState()), EventType.CHANGE_EVENT);
        } catch (DevFailed devFailed) {
            DevFailedUtils.logDevFailed(devFailed, getDevice().getLogger());
        }
    }

    public void pushStateChangeEvent(DeviceState state) {
        try {
            device.getStateImpl().stateMachine(state);
            pushEvent("State", new AttributeValue(device.getState()), EventType.CHANGE_EVENT);
        } catch (DevFailed devFailed) {
            DevFailedUtils.logDevFailed(devFailed, getDevice().getLogger());
        }
    }

    public void pushStatusChangeEvent() {
        try {
            pushEvent("Status", new AttributeValue(device.getStatus()), EventType.CHANGE_EVENT);
        } catch (DevFailed devFailed) {
            DevFailedUtils.logDevFailed(devFailed, getDevice().getLogger());
        }
    }


    public void pushStatusChangeEvent(String status) {
        try {
            device.getStatusImpl().statusMachine(status);
            pushEvent("Status", new AttributeValue(device.getStatus()), EventType.CHANGE_EVENT);
        } catch (DevFailed devFailed) {
            DevFailedUtils.logDevFailed(devFailed, getDevice().getLogger());
        }
    }
}
