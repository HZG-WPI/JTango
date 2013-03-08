//+======================================================================
// $Source$
//
// Project:   Tango
//
// Description:  java source code for the TANGO client/server API.
//
// $Author$
//
// Copyright (C) :      2004,2005,2006,2007,2008,2009,2010,2011,2012,2013
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
// $Revision$
//
//-======================================================================


package fr.esrf.TangoDs;

class KillThread extends Thread {

    public KillThread() {
	super("KillThread");
    }

    @Override
    public void run() {
	Util.out4.println("In the killer thread !!!");

	// try
	// {
	// sleep(1000);
	// }
	// catch (InterruptedException ex)
	// {
	// }

	//
	// Unregister server device(s) from database
	//

	Util.instance().unregister_server();

	//
	// Now, the suicide
	//

	System.exit(-1);
    }
}
