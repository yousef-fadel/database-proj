package com.goat.database;

import java.io.Serializable;

public class Datatype implements Comparable<Datatype>, Serializable{

	
	String str;
	Integer intValue;
	Double doubleValue;
	public Datatype(Object value)
	{
		if(value instanceof String)
			this.str = (String) value;
		
		if(value instanceof Double)
			this.doubleValue = (Double) value;
		
		if(value instanceof Integer)
			this.intValue = (Integer) value;		
	}
	
	public int compareTo(Datatype datatype)
	{
		if(str!=null)
			return this.str.compareTo(datatype.str);
		if(intValue!=null)
			return this.intValue.compareTo(datatype.intValue);
		if(doubleValue!=null)
			return this.doubleValue.compareTo(datatype.doubleValue);
		return 0;
	}
	
	
}
