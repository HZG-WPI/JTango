package fr.esrf.Tango;

/**
 * Generated from IDL struct "DevVarLongStringArray".
 *
 * @author JacORB IDL compiler V 3.1, 19-Aug-2012
 * @version generated at Dec 11, 2012 4:18:48 PM
 */

public final class DevVarLongStringArray
	implements org.omg.CORBA.portable.IDLEntity
{
	/** Serial version UID. */
	private static final long serialVersionUID = 1L;
	public DevVarLongStringArray(){}
	public int[] lvalue;
	public java.lang.String[] svalue;
	public DevVarLongStringArray(int[] lvalue, java.lang.String[] svalue)
	{
		this.lvalue = lvalue;
		this.svalue = svalue;
	}
}