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
package org.tango.server.events;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import fr.esrf.Tango.*;
import io.reactivex.Observable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;
import org.tango.client.database.DatabaseFactory;
import org.tango.network.NetworkUtils;
import org.tango.server.ServerManager;
import org.tango.server.attribute.AttributeImpl;
import org.tango.server.attribute.ForwardedAttribute;
import org.tango.server.idl.TangoIDLUtil;
import org.tango.server.pipe.PipeImpl;
import org.tango.server.pipe.PipeValue;
import org.tango.server.servant.DeviceImpl;
import org.tango.utils.DevFailedUtils;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Set of ZMQ low level utilities
 *
 * @author verdier
 */
public final class EventManager {
    private final Logger logger = LoggerFactory.getLogger(EventManager.class);
    private final XLogger xlogger = XLoggerFactory.getXLogger(EventManager.class);


    public static final int MINIMUM_IDL_VERSION = 4;
    public static final String IDL_REGEX = "idl[0-9]_[a-z]*";
    public static final String IDL_LATEST = "idl" + DeviceImpl.SERVER_VERSION + "_";
    private static final EventManager INSTANCE = new EventManager();

    private final Map<String, EventImpl> eventImplMap = new HashMap<String, EventImpl>();
    private final ScheduledExecutorService scheduledHeartbeatExecutor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                    .setNameFormat("Event-HeartBeat-%d")
                    .setDaemon(true)
                    .build());
    private final ZContext context = new ZContext();
    private final int serverHWM = initializeServerHwm();
    private final int clientHWN = initializeClientHwm();
    private final Map<String, ZMQ.Socket> heartbeatEndpoints = Maps.newLinkedHashMap();
    private final Map<String, ZMQ.Socket> eventEndpoints = Maps.newLinkedHashMap();

    private final java.util.function.Function<EventImpl, Void> pushAttributeValueEvent = (eventImpl) -> {
        for (Map.Entry<String, ZMQ.Socket> eventSocket : eventEndpoints.entrySet()) {
            try {
                logger.debug("sending event to {}", eventSocket.getKey());
                eventImpl.pushAttributeValueEvent(eventSocket.getValue());
            } catch (DevFailed devFailed) {
                logger.error("Failed to pushAttributeValueEvent");
                DevFailedUtils.logDevFailed(devFailed, logger);
            }
        }
        return null;
    };

    private EventManager() {
        List<String> ipAddresses = NetworkUtils.getInstance().getIp4Addresses();

        bindEndpoints(createSocket(), ipAddresses, heartbeatEndpoints, SocketType.HEARTBEAT);
        bindEndpoints(createEventSocket(), ipAddresses, eventEndpoints, SocketType.EVENTS);

        final String adminDeviceName = ServerManager.getInstance().getAdminDeviceName();
        final String heartbeatName;
        try {
            heartbeatName = EventUtilities.buildHeartBeatEventName(adminDeviceName);
            scheduledHeartbeatExecutor.scheduleAtFixedRate(new HeartbeatRunnable(heartbeatName), 0,
                    EventConstants.EVENT_HEARTBEAT_PERIOD, TimeUnit.MILLISECONDS);
        } catch (DevFailed devFailed) {
            logger.error("Failed to build heartbeat event name, heartbeats won't be send!");
            DevFailedUtils.logDevFailed(devFailed, logger);
        }

    }

    private int initializeServerHwm() {
        // Check the High Water Mark value from environment
        final String env = System.getenv("TANGO_DS_EVENT_BUFFER_HWM");
        try {
            if (env != null) {
                return Integer.parseInt(env);
            }
        } catch (final NumberFormatException e) {
            logger.warn("System env.TANGO_DS_EVENT_BUFFER_HWM is not a number: {} ", env);

        }
        return EventConstants.HWM_DEFAULT;
    }

    private int initializeClientHwm() {
        // Check the High Water Mark value from Control System property
        String value = "";
        try {
            value = DatabaseFactory.getDatabase().getFreeProperty("CtrlSystem", "EventBufferHwm");
            return Integer.parseInt(value);
        } catch (final DevFailed e) {
            logger.warn("Failed to get free property CtrlSystem/EventBufferHwm from the database");
            DevFailedUtils.logDevFailed(e, logger);
        } catch (final NumberFormatException e) {
            logger.warn("CtrlSystem/EventBufferHwm property is not a number: {} ", value);
        }
        return EventConstants.HWM_DEFAULT;
    }



    public static EventManager getInstance() {
        return INSTANCE;
    }

    /**
     * Check if event criteria are set for change and archive events
     *
     * @param attribute the specified attribute
     * @param eventType the specified event type
     * @throws DevFailed if event type is change or archive and no event criteria is set.
     */
    public static void checkEventCriteria(final AttributeImpl attribute, final EventType eventType) throws DevFailed {
        switch (eventType) {
            case CHANGE_EVENT:
                ChangeEventTrigger.checkEventCriteria(attribute);
                break;
            case ARCHIVE_EVENT:
                ArchiveEventTrigger.checkEventCriteria(attribute);
                break;
            default:
                break;
        }
    }

    private void bindEndpoints(ZMQ.Socket socket, Iterable<String> ipAddresses, Map<String, ZMQ.Socket> sockets, SocketType type) {
        xlogger.entry();

        final int port = socket.bindToRandomPort("tcp://*");

        String endpoint = Observable.fromIterable(ipAddresses)
                .map(s -> "tcp://" + s + ":" + port).blockingFirst();
        logger.debug("bind ZMQ socket {} for {}", endpoint, type);
        sockets.put(endpoint, socket);
        xlogger.exit();
    }

    private ZMQ.Socket createSocket(){
        final ZMQ.Socket socket = context.createSocket(ZMQ.PUB);
            socket.setLinger(0);
            socket.setReconnectIVL(-1);
            return socket ;
            }

    private ZMQ.Socket createEventSocket() {
        final ZMQ.Socket socket = context.createSocket(ZMQ.PUB);
        socket.setLinger(0);
        socket.setReconnectIVL(-1);
        socket.setSndHWM(serverHWM);
        logger.debug("HWM has been set to {}", socket.getSndHWM());
        return socket;
    }




    /**
     * Search the specified EventImpl object
     *
     * @param fullName specified EventImpl name.
     * @return the specified EventImpl object if found, otherwise returns null.
     */
    //TODO concurrency
    private EventImpl getEventImpl(final String fullName) {
        // Check if subscribed
        EventImpl eventImpl = eventImplMap.get(fullName);

        // Check if subscription is out of time
        if (eventImpl != null && !eventImpl.isStillSubscribed()) {
            logger.debug("{} not subscribed any more", fullName);
            // System.out.println(fullName + "Not Subscribed any more");
            eventImplMap.remove(fullName);

            return null;
        } else {
            return eventImpl;
        }
    }

    public boolean hasSubscriber(final String deviceName) {
        boolean hasSubscriber = false;
        for (final String eventName : eventImplMap.keySet()) {
            if (eventName.toLowerCase(Locale.ENGLISH).contains(deviceName.toLowerCase(Locale.ENGLISH))) {
                hasSubscriber = true;
                break;
            }
        }
        return hasSubscriber;
    }

    /**
     * Close all zmq resources
     */
    public void close() {
        xlogger.entry();
        logger.debug("closing all event resources");

        scheduledHeartbeatExecutor.shutdown();
        try {
            scheduledHeartbeatExecutor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            logger.error("could not stop event hearbeat");
            Thread.currentThread().interrupt();
        }

        //TODO ensure this is done in the same thread where sockets were created
            // close all open sockets
//        context.destroy();
        eventImplMap.clear();

        logger.debug("all event resources closed");
        xlogger.exit();
    }

    /**
     * returns the connection parameters for specified event.
     */
    public DevVarLongStringArray getInfo() {
        // Build the connection parameters object
        final DevVarLongStringArray longStringArray = new DevVarLongStringArray();
        // longStringArray.lvalue = new int[0];
        longStringArray.lvalue = new int[]{EventConstants.TANGO_RELEASE, DeviceImpl.SERVER_VERSION, clientHWN, 0, 0,
                EventConstants.ZMQ_RELEASE};
        if (heartbeatEndpoints.isEmpty() || eventEndpoints.isEmpty()) {
            longStringArray.svalue = new String[]{"No ZMQ event yet !"};
        } else {
            longStringArray.svalue = getEndpoints();
        }
        return longStringArray;

    }

    /**
     * Initialize ZMQ event system if not already done,
     * subscribe to the specified event end
     * returns the connection parameters for specified event.
     *
     * @param deviceName The specified event device name
     * @param pipe       The specified event pipe
     * @return the connection parameters for specified event.
     */
    public DevVarLongStringArray subscribe(final String deviceName, final PipeImpl pipe) throws DevFailed {
        xlogger.entry();
        // If first time start the ZMQ management
        // check if event is already subscribed
        final String fullName = EventUtilities.buildPipeEventName(deviceName, pipe.getName());
        EventImpl eventImpl = eventImplMap.get(fullName);
        if (eventImpl == null) {
            // If not already manage, create EventImpl object and add it to the map
            eventImpl = new EventImpl(pipe, DeviceImpl.SERVER_VERSION, fullName);
            eventImplMap.put(fullName, eventImpl);
        } else {
            eventImpl.updateSubscribeTime();
        }

        return buildConnectionParameters(fullName);
    }

    /**
     * Initialize ZMQ event system if not already done,
     * subscribe to the specified event end
     * returns the connection parameters for specified event.
     *
     * @param deviceName The specified event device name
     * @param attribute  The specified event attribute
     * @param eventType  The specified event type
     * @return the connection parameters for specified event.
     */
    public DevVarLongStringArray subscribe(final String deviceName, final AttributeImpl attribute,
                                           final EventType eventType, final int idlVersion) throws DevFailed {
        xlogger.entry();
        // check if event is already subscribed
        final String fullName = EventUtilities.buildEventName(deviceName, attribute.getName(), eventType, idlVersion);
        EventImpl eventImpl = eventImplMap.get(fullName);
        if (eventImpl == null) {
            // special case for forwarded attribute, subscribe to root attribute
            if (attribute.getBehavior() instanceof ForwardedAttribute) {
                final ForwardedAttribute fwdAttr = (ForwardedAttribute) attribute.getBehavior();
                fwdAttr.subscribe(eventType);
            }
            // If not already manage, create EventImpl object and add it to the map
            eventImpl = new EventImpl(attribute, eventType, idlVersion, fullName);
            eventImplMap.put(fullName, eventImpl);
        } else {
            eventImpl.updateSubscribeTime();
        }
        logger.debug("starting event {}", fullName);
        return buildConnectionParameters(fullName);
    }

    /**
     * Initialize ZMQ event system if not already done,
     * subscribe to the interface change event end
     * returns the connection parameters.
     *
     * @param deviceName The specified event device name
     * @return the connection parameters.
     */
    public DevVarLongStringArray subscribe(final String deviceName) throws DevFailed {
        xlogger.entry();
        // check if event is already subscribed
        final String fullName = EventUtilities.buildDeviceEventName(deviceName, EventType.INTERFACE_CHANGE_EVENT);
        EventImpl eventImpl = eventImplMap.get(fullName);
        if (eventImpl == null) {
            // If not already manage, create EventImpl object and add it to the map
            eventImpl = new EventImpl(DeviceImpl.SERVER_VERSION, fullName);
            eventImplMap.put(fullName, eventImpl);
        } else {
            eventImpl.updateSubscribeTime();
        }

        return buildConnectionParameters(fullName);
    }

    private DevVarLongStringArray buildConnectionParameters(final String fullName) {
        // Build the connection parameters object
        final DevVarLongStringArray longStringArray = new DevVarLongStringArray();
        longStringArray.lvalue = new int[]{EventConstants.TANGO_RELEASE, DeviceImpl.SERVER_VERSION, clientHWN, 0, 0,
                EventConstants.ZMQ_RELEASE};
        longStringArray.svalue = getEndpoints();
        logger.debug("event registered for {}", fullName);
        return longStringArray;
    }

    private String[] getEndpoints() {
        return Observable.zip(
                    Observable.fromIterable(heartbeatEndpoints.keySet()),
                    Observable.fromIterable(eventEndpoints.keySet()),
                    Observable::just
                )
                .flatMap(stringObservable -> stringObservable)
                .toList()
                .blockingGet().toArray(new String[0]);
    }

    /**
     * Check if the event must be sent and fire it if must be done
     *
     * @param attributeName specified event attribute
     * @param devFailed     the attribute failed error to be sent as event
     * @throws DevFailed
     */
    public void pushAttributeErrorEvent(final String deviceName, final String attributeName, final DevFailed devFailed)
            throws DevFailed {
        xlogger.entry();
        for (final EventType eventType : EventType.values()) {
            final String fullName = EventUtilities.buildEventName(deviceName, attributeName, eventType);
            final EventImpl eventImpl = getEventImpl(fullName);
            if (eventImpl != null) {
                for (ZMQ.Socket eventSocket : eventEndpoints.values()) {
                    eventImpl.pushDevFailedEvent(devFailed, eventSocket);
                }
            }
        }
        xlogger.exit();
    }

    /**
     * Check if the event must be sent and fire it if must be done
     *
     * @param attributeName specified event attribute
     * @throws DevFailed
     */
    public void pushAttributeValueEvent(final String deviceName, final String attributeName) throws DevFailed {
        xlogger.entry();
        for (final EventType eventType : EventType.getEventTypeListForAttrPolling()) {
            forEachIdlVersionDo(deviceName, attributeName, eventType, pushAttributeValueEvent);
        }
        xlogger.exit();
    }

    private void forEachIdlVersionDo(String deviceName, String attributeName, EventType eventType, java.util.function.Function<EventImpl, Void> action) throws DevFailed {
        for (int idl = MINIMUM_IDL_VERSION; idl <= DeviceImpl.SERVER_VERSION; idl++) {
            final String fullName = EventUtilities.buildEventName(deviceName, attributeName, eventType, idl);
            final EventImpl eventImpl = getEventImpl(fullName);
            if (eventImpl != null) {
                action.apply(eventImpl);
            }
        }
    }

    /**
     * fire event
     *
     * @param deviceName    Specified event device
     * @param attributeName specified event attribute name
     * @param eventType     specified event type.
     * @throws DevFailed
     */
    public void pushAttributeValueEvent(final String deviceName, final String attributeName, final EventType eventType)
            throws DevFailed {
        xlogger.entry();
        forEachIdlVersionDo(deviceName, attributeName, eventType, pushAttributeValueEvent);
        xlogger.exit();
    }

    /**
     * fire event with AttDataReady
     *
     * @param deviceName    Specified event device
     * @param attributeName specified event attribute name
     * @param counter       a counter value
     * @throws DevFailed
     */
    public void pushAttributeDataReadyEvent(final String deviceName, final String attributeName, final int counter)
            throws DevFailed {
        xlogger.entry();
        final String fullName = EventUtilities.buildEventName(deviceName, attributeName, EventType.DATA_READY_EVENT);
        final EventImpl eventImpl = getEventImpl(fullName);
        if (eventImpl != null) {
            for (ZMQ.Socket eventSocket : eventEndpoints.values()) {
                eventImpl.pushAttributeDataReadyEvent(counter, eventSocket);
            }
        }
        xlogger.exit();
    }

    public void pushAttributeConfigEvent(final String deviceName, final String attributeName) throws DevFailed {
        xlogger.entry();
        forEachIdlVersionDo(deviceName, attributeName, EventType.ATT_CONF_EVENT, (eventImpl -> {
            for (ZMQ.Socket eventSocket : eventEndpoints.values()) {
                try {
                    eventImpl.pushAttributeConfigEvent(eventSocket);
                } catch (DevFailed devFailed) {
                    logger.error("Failed to pushAttributeConfigEvent");
                    DevFailedUtils.logDevFailed(devFailed, logger);
                }
            }
            return null;
        }));
        xlogger.exit();
    }

    public void pushInterfaceChangedEvent(final String deviceName, final DevIntrChange deviceInterface)
            throws DevFailed {
        xlogger.entry();
        final String fullName = EventUtilities.buildDeviceEventName(deviceName, EventType.INTERFACE_CHANGE_EVENT);
        final EventImpl eventImpl = getEventImpl(fullName);
        if (eventImpl != null) {
            for (ZMQ.Socket eventSocket : eventEndpoints.values()) {
                eventImpl.pushInterfaceChangeEvent(deviceInterface, eventSocket);
            }
        }
        xlogger.exit();
    }

    public void pushPipeEvent(final String deviceName, final String pipeName, final PipeValue blob) throws DevFailed {
        xlogger.entry();
        final String fullName = EventUtilities.buildPipeEventName(deviceName, pipeName);
        final EventImpl eventImpl = getEventImpl(fullName);
        if (eventImpl != null) {
            for (ZMQ.Socket eventSocket : eventEndpoints.values()) {
                eventImpl.pushPipeEvent(
                        new DevPipeData(pipeName, TangoIDLUtil.getTime(blob.getTime()), blob.getValue()
                                .getDevPipeBlobObject()), eventSocket);
            }
        }
        xlogger.exit();
    }

    public void pushPipeEvent(final String deviceName, final String pipeName, final DevFailed devFailed)
            throws DevFailed {
        xlogger.entry();
        final String fullName = EventUtilities.buildPipeEventName(deviceName, pipeName);
        final EventImpl eventImpl = getEventImpl(fullName);
        if (eventImpl != null) {
            for (ZMQ.Socket eventSocket : eventEndpoints.values()) {
                eventImpl.pushDevFailedEvent(devFailed, eventSocket);
            }
        }
        xlogger.exit();
    }


    public void pushAttributeValueIDL5Event(final String deviceName, final String attributeName, AttributeValue_5 value, EventType evtType) throws DevFailed {
        xlogger.entry();
        final String fullName = EventUtilities.buildEventName(deviceName, attributeName, evtType);
        final EventImpl eventImpl = getEventImpl(fullName);
        if (eventImpl != null) {
            for (ZMQ.Socket eventSocket : eventEndpoints.values()) {
                eventImpl.pushAttributeIDL5Event(value, eventSocket);
            }
        }
        xlogger.exit();
    }

    public void pushAttributeConfigIDL5Event(final String deviceName, final String attributeName, AttributeConfig_5 config) throws DevFailed {
        xlogger.entry();
        final String fullName = EventUtilities.buildEventName(deviceName, attributeName, EventType.ATT_CONF_EVENT);
        final EventImpl eventImpl = getEventImpl(fullName);
        if (eventImpl != null) {
            for (ZMQ.Socket eventSocket : eventEndpoints.values()) {
                eventImpl.pushAttributeConfigIDL5Event(config, eventSocket);
            }
        }
        xlogger.exit();
    }

    private enum SocketType {
        HEARTBEAT, EVENTS
    }


    /**
     * This class is a thread to send a heartbeat
     */
    class HeartbeatRunnable implements Runnable {

        private final String heartbeatName;

        HeartbeatRunnable(final String heartbeatName) {
            this.heartbeatName = heartbeatName;
        }

        @Override
        public void run() {
            xlogger.entry();
            if (eventImplMap.isEmpty()) return;
            for (Map.Entry<String, ZMQ.Socket> heartbeatSocket : heartbeatEndpoints.entrySet()) {
                // Fire heartbeat
                try {
                    EventUtilities.sendHeartbeat(heartbeatSocket.getValue(), heartbeatName);
                } catch (final DevFailed e) {
                    DevFailedUtils.logDevFailed(e, logger);
                }
                logger.debug("Heartbeat sent for {} to {}", heartbeatName, heartbeatSocket.getKey());
            }
            xlogger.exit();
        }
    }

}
