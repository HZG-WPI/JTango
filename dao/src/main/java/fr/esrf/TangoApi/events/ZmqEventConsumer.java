//+======================================================================
// $Source$
//
// Project:   Tango
//
// Description:  java source code for the TANGO client/server API.
//
// $Author: pascal_verdier $
//
// Copyright (C) :      2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,
//						European Synchrotron Radiation Facility
//                      BP 220, Grenoble 38043
//                      FRANCE
//
// This file is part of Tango.
//
// Tango is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// Tango is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with Tango.  If not, see <http://www.gnu.org/licenses/>.
//
// $Revision:  $
//
//-======================================================================


package fr.esrf.TangoApi.events;


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import fr.esrf.Tango.DevError;
import fr.esrf.Tango.DevFailed;
import fr.esrf.Tango.DevVarLongStringArray;
import fr.esrf.Tango.ErrSeverity;
import fr.esrf.TangoApi.*;
import fr.esrf.TangoDs.Except;
import fr.esrf.TangoDs.TangoConst;
import io.reactivex.Observable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tango.network.NetworkUtils;
import org.tango.utils.DevFailedUtils;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author pascal_verdier
 */
public class ZmqEventConsumer {
    private static final long EVENT_RESUBSCRIBE_PERIOD = 600000;
    private static final long EVENT_HEARTBEAT_PERIOD = 10000;
    private final Logger logger = LoggerFactory.getLogger(ZmqEventConsumer.class);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                    .setNameFormat("KeepAlive-%d")
                    .setDaemon(true)
                    .build()
    );

    {
        scheduledExecutorService.scheduleAtFixedRate(new KeepAliveRunnable(), 10L, 10L, TimeUnit.SECONDS);
    }

    public ZmqEventConsumer() {
        //  Start ZMQ main thread
        new ZmqMainThread(ZmqUtils.getContext(), this).start();
    }

    private final AtomicInteger subscribe_event_id = new AtomicInteger();

    private final ConcurrentMap<String, EventChannelStruct>   channel_map        = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String>               device_channel_map = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, EventCallBackStruct>  event_callback_map = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, EventCallBackStruct>  failed_event_callback_map = new ConcurrentHashMap<>();

    //  Alternate tango hosts
    //TODO concurrent access
    private final List<String> possibleTangoHosts = new ArrayList<>();

    ConcurrentMap<String, EventChannelStruct> getChannelMap() {
        return channel_map;
    }

    ConcurrentMap<String, EventCallBackStruct>  getEventCallbackMap() {
        return event_callback_map;
    }

    /**
     * Try to connect if it failed at subscribe
     */
    private void subscribeIfNotDone() {
        for (String callbackKey : failed_event_callback_map.keySet()) {
            EventCallBackStruct eventCallBackStruct = failed_event_callback_map.get(callbackKey);
            if (eventCallBackStruct.consumer!=null) {
                try {
                    subscribeIfNotDone(eventCallBackStruct, callbackKey);
                } catch (DevFailed e) {
                    //	Send error to callback
                    sendErrorToCallback(eventCallBackStruct, callbackKey, e);
                }
            }
            else {
                try {
                    eventCallBackStruct.consumer = this;
                    subscribeIfNotDone(eventCallBackStruct, callbackKey);
                }
                catch (DevFailed e) {
                    if (e.errors[0].desc.equals(ZmqUtils.SUBSCRIBE_COMMAND_NOT_FOUND)) {
                        DevFailedUtils.logDevFailed(e, logger);
                        //  reset if both have failed
                        eventCallBackStruct.consumer = null;
                        //	Send error to callback
                        sendErrorToCallback(eventCallBackStruct, callbackKey, e);
                    }
                    else {
                        //	Send error to callback
                        eventCallBackStruct.consumer = null;
                        sendErrorToCallback(eventCallBackStruct, callbackKey, e);
                    }
                }
            }
        }
    }

    private void sendErrorToCallback(EventCallBackStruct cs, String callbackKey, DevFailed e) {

        int source = EventData.ZMQ_EVENT;
        EventData eventData =
                new EventData(cs.device, callbackKey,
                        cs.event_name, source,
                        cs.event_type, null, null, null, null, null, e.errors);

        if (cs.use_ev_queue) {
            EventQueue ev_queue = cs.device.getEventQueue();
            ev_queue.insert_event(eventData);
        } else
            cs.callback.push_event(eventData);
    }

    private void subscribeIfNotDone(EventCallBackStruct eventCallBackStruct,
                                         String callbackKey) throws DevFailed{

       eventCallBackStruct.consumer.subscribe_event(
               eventCallBackStruct.device,
               eventCallBackStruct.attr_name,
               eventCallBackStruct.event_type,
               eventCallBackStruct.callback,
               eventCallBackStruct.max_size,
               eventCallBackStruct.filters,
               false);
       failed_event_callback_map.remove(callbackKey);
    }

    private EventCallBackStruct getCallBackStruct(Map<String, EventCallBackStruct> map, int id) {
        for (String name : map.keySet()) {
            EventCallBackStruct callback_struct = map.get(name);
            if (callback_struct.id == id)
                return callback_struct;
        }
        return null;
    }

    /**
     * Subscribe event on device (Interface Change Event)
     * @param device device to subscribe
     * @param event event to subscribe
     * @param callback method to be called when receive an event
     * @param max_size  queue maximum size if use queue
     * @param stateless subscription stateless if true
     * @return the event ID
     * @throws DevFailed if subscription failed.
     */
    public int subscribe_event(DeviceProxy device,
                               int event,
                               CallBack callback,
                               int max_size,
                               boolean stateless)
            throws DevFailed {
        //	Set the event name;
        String event_name = TangoConst.eventNames[event];
        logger.debug("=============> subscribing for {}.{}",  device.name(), event_name);

        //	if no callback (null), create EventQueue
        if (callback == null && max_size >= 0) {
            //	Check if already created (in case of reconnection stateless mode)
            if (device.getEventQueue() == null)
                if (max_size > 0)
                    device.setEventQueue(new EventQueue(max_size));
                else
                    device.setEventQueue(new EventQueue());
        }

        String deviceName = device.fullName();
        String callback_key = getCallbackKey(device.name());

        //  Not added for Interface change event (Special case)
        /*
        if (device.get_idl_version()>=5)
            callback_key += ".idl" + device.get_idl_version()+ "_" + event_name;
        else
        */
            callback_key += "." + event_name;
        try {
            //	Inform server that we want to subscribe and try to connect
            logger.trace("calling callEventSubscriptionAndConnect() method");
            callEventSubscriptionAndConnect(device, event_name);
            logger.trace("call callEventSubscriptionAndConnect() method done");
        } catch (DevFailed e) {
            //  re throw if not stateless
            if (!stateless || e.errors[0].desc.equals(ZmqUtils.SUBSCRIBE_COMMAND_NOT_FOUND)) {
                throw e;
            }
            else {
                //	Build Event CallBack Structure and add it to map
                int eventId = subscribe_event_id.getAndIncrement();
                EventCallBackStruct new_event_callback_struct =
                        new EventCallBackStruct(device,
                                event_name,
                                "",
                                callback,
                                max_size,
                                eventId,
                                event,
                                false);
                failed_event_callback_map.put(callback_key, new_event_callback_struct);
                return subscribe_event_id.get();
            }
        }

        //	Prepare filters for heartbeat events on channelName
        String channelName = device_channel_map.get(deviceName);
        if (channelName==null) {
            //  If from notifd, tango host not used.
            int start = deviceName.indexOf('/', "tango:// ".length());
            deviceName = deviceName.substring(start+1);
            channelName = device_channel_map.get(deviceName);
        }
        EventChannelStruct event_channel_struct = channel_map.get(channelName);
        event_channel_struct.last_subscribed = System.currentTimeMillis();

        //	Check if a new event or a re-trying one
        int eventId;
        EventCallBackStruct failed_struct = failed_event_callback_map.get(callback_key);
        if (failed_struct == null) {
            //	It is a new one
            eventId = subscribe_event_id.incrementAndGet();
        } else
            eventId = failed_struct.id;

        //	Build Event CallBack Structure if any
        EventCallBackStruct new_event_callback_struct =
                new EventCallBackStruct(device,
                        event_name,
                        channelName,
                        callback,
                        max_size,
                        eventId,
                        event,
                        true);
        new_event_callback_struct.consumer  = this;
        event_callback_map.put(callback_key, new_event_callback_struct);

        return eventId;
    }

    private void callEventSubscriptionAndConnect(DeviceProxy device, String eventType)
            throws DevFailed {
        //  Done for IDL>=5 and not for notifd event system (no attribute name)
        String device_name = device.name();
        String[] info = new String[] {
                device_name,
                "",
                "subscribe",
                eventType,
                Integer.toString(device.get_idl_version())
        };
        //TODO extract into a class ZmqConnector
        DeviceData argIn = new DeviceData();
        argIn.insert(info);
        String cmdName = ZmqUtils.SUBSCRIBE_COMMAND;
        logger.trace("{}.command_inout({}) for {}.{}", device.get_adm_dev().name(), cmdName, device_name, eventType);
        DeviceData argOut =
                device.get_adm_dev().command_inout(cmdName, argIn);
        logger.trace("    command_inout done.");

        //	And then connect to device
        checkDeviceConnection(device, null, argOut, eventType);
    }

    private void setAdditionalInfoToEventCallBackStruct(EventCallBackStruct callback_struct,
                                                        String device_name, String attribute, String event_name, String[] filters, EventChannelStruct channel_struct) {
        // Nothing
        logger.debug("-------------> Set as ZmqEventConsumer for {}", device_name);
        callback_struct.consumer  = this;
    }

    private void connect(DeviceProxy deviceProxy, String attributeName,
                         String eventName, DeviceData deviceData) throws DevFailed {
        String deviceName = deviceProxy.fullName();
        int tangoVersion = deviceData.extractLongStringArray().lvalue[0];
        try {
            String adminName = deviceProxy.adm_name();  //.toLowerCase();
            //  Since Tango 8.1, heartbeat is sent in lower case.
            //tangoVersion = new DeviceProxy(adm_name).getTangoVersion();
            if (tangoVersion>=810)
                adminName = adminName.toLowerCase();

            // If no connection exists to this channel, create it
            Database database = null;
            if (!channel_map.containsKey(adminName)) {
                if (deviceProxy.use_db())
                    database = deviceProxy.get_db_obj();
                ConnectionStructure connectionStructure =
                        new ConnectionStructure(deviceProxy.get_tango_host(),
                                adminName, deviceName, attributeName,
                                eventName, database, deviceData, false);
                connect_event_channel(connectionStructure);
            } else if (deviceProxy.use_db()) {
                database = deviceProxy.get_db_obj();
                ZmqUtils.connectEvent(deviceName,
                        attributeName, deviceData.extractLongStringArray(), eventName);
            }
            EventChannelStruct eventChannelStruct = channel_map.get(adminName);
            eventChannelStruct.adm_device_proxy =  new DeviceProxy(adminName);
            eventChannelStruct.use_db = deviceProxy.use_db();
            eventChannelStruct.dbase = database;
            eventChannelStruct.setTangoRelease(tangoVersion);

            device_channel_map.put(deviceName, adminName);
        }
        catch (DevFailed e) {
            Except.throw_event_system_failed("API_BadConfigurationProperty",
                    "Can't subscribe to event for device " + deviceName
                            + "\n Check that device server is running...",
                    "ZmqEventConsumer.connect");
        }
    }

    /**
     *  Due to a problem when there is more than one network card,
     *  The address returned by the command ZmqEventSubscriptionChange
     *  is different than the getHostAddress() call !!!
     *  In this case the address from getHostAddress()
     *  replace the address in device data.
     */
    private DeviceData checkWithHostAddress(DeviceData deviceData, DeviceProxy deviceProxy) {
// ToDo
        DevVarLongStringArray lsa = deviceData.extractLongStringArray();
        try {
            java.net.InetAddress iadd =
                    java.net.InetAddress.getByName(deviceProxy.get_host_name());
            String hostAddress = iadd.getHostAddress();
            logger.debug("Host address is {}", hostAddress);
            logger.debug("Server returns  {}", lsa.svalue[0]);
            if (! lsa.svalue[0].startsWith("tcp://"+hostAddress)) { //  Addresses are different
                 String  wrongAdd = lsa.svalue[0];
                 int idx = lsa.svalue[0].lastIndexOf(':');   //  get port
                 if (idx>0) {
                     lsa.svalue[0] = "tcp://" + hostAddress + lsa.svalue[0].substring(idx);
                     lsa.svalue[1] = "tcp://" + hostAddress + lsa.svalue[1].substring(idx);
                     logger.debug("{} ---> {}", wrongAdd, lsa.svalue[0]);
                     deviceData = new DeviceData();
                     deviceData.insert(lsa);
                     NetworkUtils.getInstance().checkEndpoint(lsa.svalue[0]);
                 }
            }
        } catch (UnknownHostException e) {
            logger.warn("UnknownHostException in ZmqEventConsumer.checkZmqAddress()", e);
        } catch (DevFailed devFailed) {
            DevFailedUtils.logDevFailed(devFailed, logger);
        }

        return deviceData;
    }

    /**
     * In case of several endpoints, check which one is connected.
     * @param deviceData    data from ZmqEventSubscriptionChange command
     * @param deviceProxy   the admin device
     * @return the endpoints after checked
     * @throws DevFailed in case of connection problem
     */
    private DeviceData checkZmqAddress(DeviceData deviceData, DeviceProxy deviceProxy) throws DevFailed{
        logger.trace("Inside checkZmqAddress()");
        DevVarLongStringArray lsa = deviceData.extractLongStringArray();

        Pair<String, String> validEndpoints = Optional.ofNullable(Observable.zip(
                Observable.fromArray(lsa.svalue),
                Observable.fromArray(lsa.svalue).skip(1),
                ImmutablePair::of
        )
                .filter(pair -> NetworkUtils.getInstance().checkEndpoint(pair.left))
                .blockingFirst(null))
                .orElseGet(() -> {
                    DeviceData checkedWithHostAddress = checkWithHostAddress(deviceData, deviceProxy);
                    DevVarLongStringArray yalsa = checkedWithHostAddress.extractLongStringArray();
                    return ImmutablePair.of(yalsa.svalue[0], yalsa.svalue[1]);
                });

        logger.debug("---> Heartbeat connect to {}", validEndpoints.getLeft());
        logger.debug("---> Event connect to {}", validEndpoints.getRight());

        DeviceData overridden = new DeviceData();
        //TODO refactor to have more robust solution
        lsa.svalue = new String[]{validEndpoints.getLeft(), validEndpoints.getRight()};
        overridden.insert(lsa);
        return overridden;
    }



    private void checkDeviceConnection(DeviceProxy deviceProxy,
                                       String attribute, DeviceData deviceData, String event_name) throws DevFailed {

        //  Check if address is coherent (??)
        deviceData = checkZmqAddress(deviceData, deviceProxy);

        String deviceName = deviceProxy.fullName();
        logger.debug("checkDeviceConnection for {}", deviceName);
        if (!device_channel_map.containsKey(deviceName)) {
            logger.debug("device_channel_map has no entity for {}", deviceName);
            connect(deviceProxy, attribute, event_name, deviceData);
            if (!device_channel_map.containsKey(deviceName)) {
                Except.throw_event_system_failed("API_NotificationServiceFailed",
                        "Failed to connect to event channel for device",
                        "EventConsumer.subscribe_event()");
            }
        }
        else {
            logger.debug("{} already connected.", deviceName);
            ZmqUtils.connectEvent(deviceName,
                        attribute, deviceData.extractLongStringArray(), event_name);
        }
    }

    private synchronized void connect_event_channel(ConnectionStructure cs) throws DevFailed {
        //	Get a reference to an EventChannel for
        //  this device server from the tango database
        DeviceProxy adminDevice = new DeviceProxy(cs.channelName);
        cs.channelName = adminDevice.fullName().toLowerCase();    //  Update name with tango host

        DevVarLongStringArray   lsa = cs.deviceData.extractLongStringArray();
        logger.debug("connect_event_channel for {}", cs.channelName);

        //  Build the buffer to connect heartbeat and send it
        ZmqUtils.connectHeartbeat(adminDevice.name(), lsa);

        //  Build the buffer to connect event and send it
        ZmqUtils.connectEvent(cs.deviceName, cs.attributeName,
                lsa, cs.eventName);
        if (cs.reconnect) {
            EventChannelStruct eventChannelStruct = channel_map.get(cs.channelName);
           // eventChannelStruct.eventChannel = eventChannel;
            eventChannelStruct.last_heartbeat = System.currentTimeMillis();
            eventChannelStruct.heartbeat_skipped = false;
            eventChannelStruct.has_notifd_closed_the_connection = 0;
            eventChannelStruct.setTangoRelease(lsa.lvalue[0]);
            eventChannelStruct.setIdlVersion(lsa.lvalue[1]);
        } else {
            //  Crate new one
            EventChannelStruct newEventChannelStruct = new EventChannelStruct();
            //newEventChannelStruct.eventChannel = eventChannel;
            newEventChannelStruct.last_heartbeat = System.currentTimeMillis();
            newEventChannelStruct.heartbeat_skipped = false;
            newEventChannelStruct.adm_device_proxy = adminDevice;
            newEventChannelStruct.has_notifd_closed_the_connection = 0;
            newEventChannelStruct.consumer = this;
            newEventChannelStruct.zmqEndpoint = lsa.svalue[0];
            newEventChannelStruct.setTangoRelease(lsa.lvalue[0]);
            newEventChannelStruct.setIdlVersion(lsa.lvalue[1]);
            channel_map.put(cs.channelName, newEventChannelStruct);
            ApiUtil.printTrace("Adding " + cs.channelName + " to channel_map");

            //  Get possible TangoHosts and add it to list if not already in.
            String[]    tangoHosts = adminDevice.get_db_obj().getPossibleTangoHosts();
            for (String tangoHost : tangoHosts) {
                tangoHost = "tango://" + tangoHost;
                for (String possibleTangoHost : possibleTangoHosts) {
                    if (possibleTangoHost.equals(tangoHost))
                        possibleTangoHosts.add(tangoHost);
                }
            }
        }
    }

    private boolean reSubscribe(EventChannelStruct channelStruct, EventCallBackStruct eventCallBackStruct) {
        //  ToDo
        boolean done = false;
        try {
            logger.debug("====================================================\n" +
                                "   Try to resubscribe {}", eventCallBackStruct.channel_name);
            DeviceData argOut = ZmqUtils.getEventSubscriptionInfoFromAdmDevice(
                        channelStruct.adm_device_proxy,
                        eventCallBackStruct.device.name(),
                        eventCallBackStruct.attr_name, eventCallBackStruct.event_name);
            DevVarLongStringArray lsa = checkZmqAddress(argOut, eventCallBackStruct.device).extractLongStringArray();

            //  Update the heartbeat time
            String  admDeviceName = channelStruct.adm_device_proxy.name();  //.toLowerCase();
            //  Since Tango 8.1, heartbeat is sent in lower case.
            if (channelStruct.getTangoRelease()>=810)
                admDeviceName = admDeviceName.toLowerCase();
            push_structured_event_heartbeat(admDeviceName);
            channelStruct.heartbeat_skipped = false;
            channelStruct.last_subscribed = System.currentTimeMillis();
            channelStruct.setTangoRelease(lsa.lvalue[0]);
            channelStruct.setIdlVersion(lsa.lvalue[1]);
            eventCallBackStruct.last_subscribed = channelStruct.last_subscribed;
            done = true;
        }
        catch(DevFailed e) {
            logger.warn(DevFailedUtils.toString(e));
        }
        return done;
    }

    void checkIfHeartbeatSkipped(String name, EventChannelStruct channelStruct) {
            // Check if heartbeat have been skipped, can happen if
            // 1- the server is dead
            // 2- The network was down;
            // 3- The server has been restarted on another host.

        if (((System.currentTimeMillis() - channelStruct.last_heartbeat) > EVENT_HEARTBEAT_PERIOD)) {
            DevError    dev_error = null;
            try{
                String  admDeviceName = channelStruct.adm_device_proxy.fullName();  //.toLowerCase();
                //  Since Tango 8.1, heartbeat is sent in lower case.
                if (channelStruct.getTangoRelease()>=810)
                    admDeviceName = admDeviceName.toLowerCase();
                channelStruct.adm_device_proxy = new DeviceProxy(admDeviceName);
                channelStruct.adm_device_proxy.set_timeout_millis(300);
                channelStruct.adm_device_proxy.ping();
                reconnectToChannel(name);
            }
            catch (DevFailed e) {
                dev_error = e.errors[0];
            }

            for (EventCallBackStruct callbackStruct : getEventCallbackMap().values()) {
                if (callbackStruct.channel_name.equals(name)) {
                    //	Push exception
                    if (dev_error != null)
                        pushReceivedException(channelStruct, callbackStruct, dev_error);
                    else {
                        if (!reconnectToEvent(channelStruct, callbackStruct)) {
                            dev_error = new DevError("API_NoHeartbeat",
                                    ErrSeverity.ERR, "No heartbeat from " +
                                    channelStruct.adm_device_proxy.get_name(),
                                    "ZmqEventConsumer.checkIfHeartbeatSkipped()");
                            pushReceivedException(channelStruct, callbackStruct, dev_error);
                        }
                    }
                }
            }
        }
    }

    private void unsubscribeTheEvent(EventCallBackStruct callbackStruct) throws DevFailed {
        ZmqUtils.disConnectEvent(callbackStruct.device.get_tango_host(),
                callbackStruct.device.name(),
                callbackStruct.attr_name,
                callbackStruct.device.get_idl_version(),
                callbackStruct.event_name);
    }

    /**
     * Reconnect to event
     *
     * @return true if reconnection done
     */
    private boolean reconnectToEvent(EventChannelStruct channelStruct, EventCallBackStruct callBackStruct) {
        boolean reConnected;
        try {
            DeviceData argOut = ZmqUtils.getEventSubscriptionInfoFromAdmDevice(
                        channelStruct.adm_device_proxy,
                        callBackStruct.device.name(),
                        callBackStruct.attr_name,
                        callBackStruct.event_name);
            DevVarLongStringArray   lsa = checkZmqAddress(argOut, callBackStruct.device).extractLongStringArray();

            //  Build the buffer to connect event and send it
            ZmqUtils.connectEvent(
                    callBackStruct.device.name(),
                    callBackStruct.attr_name, lsa,
                    callBackStruct.event_name);
            reConnected = true;
        }
        catch (DevFailed e) {
            logger.warn(DevFailedUtils.toString(e));
            reConnected = false;
        }
        return reConnected;
    }

    /**
     * Reconnect to channel
     *
     * @param name channel name
     * @return true if reconnection done
     */
    private boolean reconnectToChannel(String name) {
        boolean reConnected = false;
        for (EventCallBackStruct eventCallBackStruct : event_callback_map.values()) {
            if (eventCallBackStruct.channel_name.equals(name) && (eventCallBackStruct.callback != null)) {
                try {
                    EventChannelStruct channelStruct = channel_map.get(name);
                    DeviceData argOut = ZmqUtils.getEventSubscriptionInfoFromAdmDevice(
                            channelStruct.adm_device_proxy, eventCallBackStruct.device.name(),
                            eventCallBackStruct.attr_name, eventCallBackStruct.event_name);
                    DevVarLongStringArray lsa = checkZmqAddress(
                            argOut, eventCallBackStruct.device).extractLongStringArray();

                    //  Re Connect heartbeat
                    ZmqUtils.connectHeartbeat(
                            channelStruct.adm_device_proxy.name(), lsa);
                    reConnected = true;
                } catch (DevFailed e1) {
                    //Except.print_exception(e1);
                    reConnected = false;
                }
                break;
            }
        }
        return reConnected;
    }

    private EventChannelStruct getEventChannelStruct(String channelName) {
        if (channel_map.containsKey(channelName)) {
            return channel_map.get(channelName);
        }
        //  Check with other TangoHosts using possibleTangoHosts as header
        int index = channelName.indexOf("//");
        if (index>0) {
            index = channelName.indexOf('/', index+2); //  "//".length()
            for (String possibleTangoHost : possibleTangoHosts) {
                String key = possibleTangoHost + channelName.substring(index);
                if (channel_map.containsKey(key))
                    return channel_map.get(key);
            }
        }
        return null;
    }

    /*private*/ void push_structured_event_heartbeat(String channelName) {
        //  ToDo
        try {
            //  If full name try to get structure from channel_map with different tango hosts
            if (channelName.startsWith("tango://")) {
                EventChannelStruct  eventChannelStruct = getEventChannelStruct(channelName);
                if (eventChannelStruct!=null) {
                    eventChannelStruct.last_heartbeat = System.currentTimeMillis();
                    return;
                }
            }

            //  Not Found !
            //	In case of (use_db==false)
            //	domain name is only device name
            //	but key is full name (//host:port/a/b/c....)
            for (String name : channel_map.keySet()) {
                EventChannelStruct eventChannelStruct = channel_map.get(name);
                //  Check with device name for adm device
                String  admDeviceName = eventChannelStruct.adm_device_proxy.name();
                if (admDeviceName.equalsIgnoreCase(channelName)) {
                    eventChannelStruct.last_heartbeat = System.currentTimeMillis();
                    break;
                }
            }
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }

    private void callEventSubscriptionAndConnect(DeviceProxy device,
                                                 String attribute,
                                                 String eventType) throws DevFailed {

        String device_name = device.name();
        if (attribute==null) // Interface change event on device.
            attribute = "";
        String[] info = new String[] {
                device_name,
                attribute,
                "subscribe",
                eventType,
                Integer.toString(device.get_idl_version())
        };
        DeviceData argIn = new DeviceData();
        argIn.insert(info);
        String cmdName = ZmqUtils.SUBSCRIBE_COMMAND;
        logger.debug("{}.command_inout({}) for {}/{}.{}", device.get_adm_dev().name(), cmdName, device_name, attribute, eventType);
        DeviceData argOut = device.get_adm_dev().command_inout(cmdName, argIn);
        logger.trace("    command_inout done.");

        //	And then connect to device
        checkDeviceConnection(device, attribute, argOut, eventType);
    }

    public int subscribe_event(DeviceProxy device,
                               String attribute,
                               int event,
                               CallBack callback,
                               String[] filters,
                               boolean stateless)
            throws DevFailed {
        return subscribe_event(device, attribute, event, callback, -1, filters, stateless);
    }

    public int subscribe_event(DeviceProxy device,
                               String attribute,
                               int event,
                               int max_size,
                               String[] filters,
                               boolean stateless)
            throws DevFailed {
        return subscribe_event(device, attribute, event, null, max_size, filters, stateless);
    }

    /**
     *
     * @param device
     * @param attribute
     * @param event
     * @param callback
     * @param max_size
     * @param filters
     * @param stateless -- throw DevFailed if subscription has failed
     * @return
     * @throws DevFailed
     */
    public int subscribe_event(DeviceProxy device,
                               String attribute,
                               int event,
                               CallBack callback,
                               int max_size,
                               String[] filters,
                               boolean stateless)
            throws DevFailed {
        //	Set the event name;
        String event_name = TangoConst.eventNames[event];
        logger.debug("=============> subscribing for {}{}.{}", device.name(),((attribute==null)? "" : "/" + attribute), event_name);

        //	if no callback (null), create EventQueue
        if (callback == null && max_size >= 0) {
            //	Check if already created (in case of reconnection stateless mode)
            if (device.getEventQueue() == null)
                if (max_size > 0)
                    device.setEventQueue(new EventQueue(max_size));
                else
                    device.setEventQueue(new EventQueue());
        }

        String deviceName = device.fullName();
        String callback_key = getCallbackKey(device.name());
        try {
            //  Check idl version
            if (device.get_idl_version()>=5) {
                switch (event_name) {
                    //TODO enum
                    case "intr_change":
                        //  No IDL for interface change
                        callback_key += "." + event_name;
                        break;
                    case "pipe":
                        //    No IDL for pipe
                        callback_key += "/" + attribute + "." + event_name;
                        break;
                    default:
                        callback_key += "/" + attribute + ".idl" + device.get_idl_version() + "_" + event_name;
                        break;
                }
            }
            else
                callback_key += "/" + attribute + "." + event_name;

            //	Inform server that we want to subscribe and try to connect
            logger.trace("calling callEventSubscriptionAndConnect() method");
            String  att = (attribute==null)? null : attribute.toLowerCase();
            callEventSubscriptionAndConnect(device, att, event_name);
            logger.trace("call callEventSubscriptionAndConnect() method done");
        } catch (DevFailed e) {
            //  re throw if not stateless
            if (!stateless || e.errors[0].desc.equals(ZmqUtils.SUBSCRIBE_COMMAND_NOT_FOUND)) {
                throw e;
            }
            else {
                //	Build Event CallBack Structure and add it to map
                int eventId = subscribe_event_id.incrementAndGet();
                EventCallBackStruct new_event_callback_struct =
                        new EventCallBackStruct(device,
                                attribute,
                                event_name,
                                "",
                                callback,
                                max_size,
                                eventId,
                                event,
//                                "",
//                                -1,
                                filters,
                                false);
                failed_event_callback_map.put(callback_key, new_event_callback_struct);
                return eventId;
            }
        }

        //	Prepare filters for heartbeat events on channelName
        String channelName = device_channel_map.get(deviceName);
        if (channelName==null) {
            //  If from notifd, tango host not used.
            int start = deviceName.indexOf('/', "tango:// ".length());
            deviceName = deviceName.substring(start+1);
            channelName = device_channel_map.get(deviceName);
        }
        EventChannelStruct event_channel_struct = channel_map.get(channelName);
        event_channel_struct.last_subscribed = System.currentTimeMillis();

        //	Check if a new event or a re-trying one
        int evnt_id;
        EventCallBackStruct failed_struct = failed_event_callback_map.get(callback_key);
        if (failed_struct == null) {
            //	It is a new one
            evnt_id = subscribe_event_id.incrementAndGet();
        } else
            evnt_id = failed_struct.id;

        //	Build Event CallBack Structure if any
        EventCallBackStruct new_event_callback_struct =
                new EventCallBackStruct(device,
                        attribute,
                        event_name,
                        channelName,
                        callback,
                        max_size,
                        evnt_id,
                        event,
//                        constraint_expr,
//                        filter_id,
                        filters,
                        true);
        setAdditionalInfoToEventCallBackStruct(new_event_callback_struct,
                deviceName, attribute, event_name, filters, event_channel_struct);
        event_callback_map.put(callback_key, new_event_callback_struct);

        return evnt_id;
    }

    private String getCallbackKey(String deviceName) {
        return "tango://localhost:10000/" + deviceName.toLowerCase();
    }

    private void removeCallBackStruct(Map<String, ?> map, EventCallBackStruct cb_struct) {
        String callback_key = cb_struct.device.name().toLowerCase() +
                "/" + cb_struct.attr_name + "." + cb_struct.event_name;
        map.remove(callback_key);
    }

    public void unsubscribe_event(int event_id) throws DevFailed {
        //	Get callback struct for event ID
        EventCallBackStruct callbackStruct =
                getCallBackStruct(event_callback_map, event_id);
        if (callbackStruct != null) {
            removeCallBackStruct(event_callback_map, callbackStruct);
            unsubscribeTheEvent(callbackStruct);
        }
        else {
            //	If not found check if in failed map
            callbackStruct =
                    getCallBackStruct(failed_event_callback_map, event_id);
            if (callbackStruct != null)
                removeCallBackStruct(failed_event_callback_map, callbackStruct);
            else
                Except.throw_event_system_failed("API_EventNotFound",
                        "Failed to unsubscribe event, the event id (" + event_id +
                                ") specified does not correspond with any known one",
                        "EventConsumer.unsubscribe_event()");
        }
    }

    /*
     * Re subscribe event selected by name
     */
     void reSubscribeByName(EventChannelStruct event_channel_struct, String name) {
        for (EventCallBackStruct callback_struct : event_callback_map.values()) {
            if (callback_struct.channel_name.equals(name)) {
                reSubscribe(event_channel_struct, callback_struct);
            }
        }
    }

    /*
     * Push event containing exception
     */
    private void pushReceivedException(EventChannelStruct event_channel_struct, EventCallBackStruct callback_struct, DevError error) {
        int eventSource = EventData.ZMQ_EVENT;
        DevError[] errors = {error};
        String domain_name = callback_struct.device.name();
        if (callback_struct.attr_name!=null)
            domain_name += "/" + callback_struct.attr_name.toLowerCase();
        EventData event_data =
                new EventData(event_channel_struct.adm_device_proxy,
                        domain_name, callback_struct.event_name, callback_struct.event_type,
                        eventSource, null, null, null, null, null, errors);

        CallBack callback = callback_struct.callback;
        event_data.device = callback_struct.device;
        event_data.name = callback_struct.device.name();
        event_data.event = callback_struct.event_name;

        if (callback_struct.use_ev_queue) {
            EventQueue ev_queue = callback_struct.device.getEventQueue();
            ev_queue.insert_event(event_data);
        } else
            callback.push_event(event_data);
    }

    protected static class ConnectionStructure {
        String      tangoHost;
        String      channelName;
        String      attributeName;
        String      deviceName;
        String      eventName;
        Database    database;
        DeviceData  deviceData = null;
        boolean     reconnect = false;
        //===========================================================
        ConnectionStructure(String tangoHost,
                            String channelName,
                            String deviceName,
                            String attributeName,
                            String eventName,
                            Database database,
                            DeviceData deviceData,
                            boolean reconnect) {
            this.tangoHost      = tangoHost;
            this.channelName    = channelName;
            this.deviceName     = deviceName;
            this.attributeName  = attributeName;
            this.eventName      = eventName;
            this.database          = database;
            this.deviceData     = deviceData;
            this.reconnect      = reconnect;
        }
        //===========================================================
        ConnectionStructure(String tangoHost, String name, Database dbase, boolean reconnect) {
            this(tangoHost, name, null, null, null, dbase, null, reconnect);
        }
        //===========================================================
        public String toString() {
            return "channel name: " + channelName +
                 "\ndatabase:     " + database +
                 "\nreconnect:    " + reconnect;
        }
        //===========================================================
    }

    public List<String> getPossibleTangoHosts() {
        return possibleTangoHosts;
    }

    /**
     * A class inherited from TimerTask class
     *
     * @author pascal_verdier
     */
    class KeepAliveRunnable implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(KeepAliveRunnable.class);

        private final ZmqEventConsumer consumer = ZmqEventConsumer.this;

        public void run() {
            try {
                consumer.subscribeIfNotDone();
                resubscribe_if_needed();
            } catch (Exception | Error err) {
                logger.warn(err.getMessage(), err);
            }
        }

        private void resubscribe_if_needed() {
            long now = System.currentTimeMillis();

            // check the list of not yet connected events and try to subscribe
            for (String name : consumer.getChannelMap().keySet()) {
                EventChannelStruct eventChannelStruct = consumer.getChannelMap().get(name);
                if ((now - eventChannelStruct.last_subscribed) > EVENT_RESUBSCRIBE_PERIOD / 3) {
                    reSubscribeByName(eventChannelStruct, name);
                }
                eventChannelStruct.consumer.checkIfHeartbeatSkipped(name, eventChannelStruct);

            }// end while  channel_names.hasMoreElements()
        }

        /*
         * Re subscribe event selected by name
         */
        private void reSubscribeByName(EventChannelStruct eventChannelStruct, String name) {

            //  Get the map and the callback structure for channel
            Map<String, EventCallBackStruct>
                    callBackMap = consumer.getEventCallbackMap();
            EventCallBackStruct callbackStruct = null;
            for (String key : callBackMap.keySet()) {
                EventCallBackStruct eventStruct = callBackMap.get(key);
                if (eventStruct.channel_name.equals(name)) {
                    callbackStruct = eventStruct;
                }
            }

            //  Get the callback structure
            if (callbackStruct != null) {
                callbackStruct.consumer.reSubscribeByName(eventChannelStruct, name);
            }
        }
    }
}
