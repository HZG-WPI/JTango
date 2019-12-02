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
package org.tango.server.annotation;

import org.tango.DeviceState;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 * Manage state of the device.
 * </p>
 *
 * <pre>
 * &#064;State
 * private {@link DeviceState} state;
 *
 * public {@link DeviceState} getState(){...}
 *
 * public void setState({@link DeviceState} state){...}
 * </pre>
 * <p>
 * A class may not declare more than one <tt>&#064;State</tt> method.
 * </p>
 *
 * @author ABEILLE
 *
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface State {
    /**
     * define if attribute is polled. period must be configured. see {@link Attribute#pollingPeriod()}
     *
     * @return is polled
     */
    boolean isPolled() default false;

    /**
     * Configure polling period in ms. use only is {@link Attribute#isPolled()} is true
     *
     * @return polling period
     */
    int pollingPeriod() default 0;

    /**
     * The framework will check event conditions before firing it
     *
     * @return
     */
    boolean checkChangeEvent() default false;
}
