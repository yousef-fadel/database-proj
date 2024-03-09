package com.goat.database;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;
import java.util.ArrayList;
public class Tuple implements java.io.Serializable {
	Object Primary_key;
	Hashtable<String, Object> entry;
	public Tuple() {

	}
	public Tuple(Object Primary_key,Hashtable<String, Object> entry) {
		this.entry=entry;
		this.Primary_key=Primary_key;
	}
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		for (String key : entry.keySet()) {
			stringBuilder.append(entry.get(key));
			stringBuilder.append(",");
		}
		if (stringBuilder.length() > 0) {
			stringBuilder.deleteCharAt(stringBuilder.length() - 1);
		}


		return stringBuilder.toString();

	}
	public static void main(String[]args)
	{
		Hashtable htblColNameType = new Hashtable( );
		htblColNameType.put("id", new Integer( 2343432 ));
		htblColNameType.put("name", new String("Ahmed Noor" ) );
		htblColNameType.put("gpa", new Double( 0.95 ) );


		Tuple tuple = new Tuple( 2343432, htblColNameType);
		System.out.println(tuple);
	}
}

