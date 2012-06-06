/**
 * Copyright (C) :     2012
 *
 * 	Synchrotron Soleil
 * 	L'Orme des merisiers
 * 	Saint Aubin
 * 	BP48
 * 	91192 GIF-SUR-YVETTE CEDEX
 *
 * This file is part of Tango.
 *
 * Tango is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Tango is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with Tango.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.tango.server.testserver;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.tango.server.ServerManager;
import org.tango.server.servant.Constants;

import fr.esrf.Tango.DevFailed;

/**
 * Starts, stop the server JTangoTest without tango db
 * 
 * @author ABEILLE
 * 
 */
public class NoDBDeviceManager {

    public static final String deviceName = "tango://localhost:" + JTangoTest.NO_DB_GIOP_PORT + "/"
	    + JTangoTest.NO_DB_DEVICE_NAME + "#dbase=no";

    public static String adminName;

    @BeforeClass
    public static void startDevice() throws DevFailed {
	JTangoTest.startNoDb();
	adminName = "tango://localhost:" + JTangoTest.NO_DB_GIOP_PORT + "/" + Constants.ADMIN_DEVICE_DOMAIN + "/"
		+ ServerManager.getInstance().getServerName() + "#dbase=no";
    }

    @AfterClass
    public static void stopDevice() throws DevFailed {
	ServerManager.getInstance().stop();
    }

}
