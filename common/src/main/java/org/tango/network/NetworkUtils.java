package org.tango.network;

import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 18.02.2020
 */
public class NetworkUtils {
    private static final NetworkUtils INSTANCE = new NetworkUtils();
    private final Logger logger = LoggerFactory.getLogger(NetworkUtils.class);

    private NetworkUtils() {
    }

    public static NetworkUtils getInstance() {
        return INSTANCE;
    }

    public int getRandomPort() {
        //TODO check if available
        return new Random().ints(5000, 65535).findFirst().getAsInt();
    }

    public List<String> getIp4Addresses() {
        Iterable<NetworkInterface> networkInterfaces = null;
        try {
            networkInterfaces = Collections.list(NetworkInterface.getNetworkInterfaces());
        } catch (SocketException e) {
            logger.error("Failed to get NICs due to " + e.getMessage(), e);
            return Collections.emptyList();
        }

        java.util.function.Predicate<NetworkInterface> filter = networkInterface -> {
            try {
                return !networkInterface.isLoopback() && !networkInterface.isVirtual() && networkInterface.isUp();
            } catch (SocketException e) {
                logger.warn("Ignoring NetworkInterface({}) due to an exception: {}", networkInterface.getName(), e);
                return false;
            }
        };

        Function<InterfaceAddress, String> interfaceAddressToString = interfaceAddress -> interfaceAddress.getAddress().getHostAddress();

        Iterable<NetworkInterface> filteredNICs = Iterables.filter(networkInterfaces, filter::test);

        List<String> result = Lists.newArrayList();
        //TODO #17
        for (NetworkInterface nic : filteredNICs) {
            result.addAll(
                    Collections2.filter(
                            nic.getInterfaceAddresses()
                                    .stream()
                                    .map(interfaceAddressToString::apply)
                                    .collect(Collectors.toList()),
                            s -> s.split("\\.").length == 4)
            );
        }
        return result;
    }

    public boolean checkEndpoint(String endpoint) {
        logger.debug("Check endpoint: {}", endpoint);
        URI uri = null;
        try {
            uri = new URI(endpoint);
        } catch (URISyntaxException e) {
            logger.debug("Bad endpoint: " + endpoint, e);
            return false;
        }

        //  Try to connect
        InetSocketAddress ip = new InetSocketAddress(uri.getHost(), uri.getPort());
        try (Socket socket = new Socket()) {
            socket.connect(ip, 10);
            return true;
        } catch (IOException e) {
            logger.debug("Failed to connect to " + ip, e);
            return false;
        }
    }
}
