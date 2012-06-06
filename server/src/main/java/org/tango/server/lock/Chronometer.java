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
package org.tango.server.lock;

/**
 * Calculate elapsed time
 * 
 * @author ABEILLE
 * 
 */
public final class Chronometer {

    private static final int TO_SECS = 1000;
    private long startTime = 0;
    private long duration = 0;
    private boolean isOver = true;

    /**
     * Start the chronometer
     * 
     * @param duration
     *            the duration in seconds
     */
    public synchronized void start(final long duration) {
	startTime = System.currentTimeMillis();
	this.duration = duration;
	isOver = false;
    }

    /**
     * stop the chronometer
     */
    public synchronized void stop() {
	isOver = true;
    }

    /**
     * Check if the started duration is over
     * 
     * @return true if over
     */
    public synchronized boolean isOver() {
	if (!isOver) {
	    final long now = System.currentTimeMillis();
	    if (now - startTime > duration * TO_SECS) {
		isOver = true;
	    }
	}
	return isOver;
    }
}
