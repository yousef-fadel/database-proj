package com.goat.database;
import java.io.Serializable;
import java.util.Hashtable;
public class Tuple implements Serializable, Comparable {
	Object Primary_key;
	Hashtable<String, Object> entry;
	

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
	@Override
	public int compareTo(Object o) {
		Tuple compare = (Tuple) o;
		if(compare.Primary_key instanceof String)
		{
			String s1 = (String) this.Primary_key;
			String s2 = (String) compare.Primary_key;
			return s1.compareTo(s2);
		}
		if(compare.Primary_key instanceof Integer)
		{
			Integer s1 = (Integer) this.Primary_key;
			Integer s2 = (Integer) compare.Primary_key;
			return s1.compareTo(s2);
		}
		if(compare.Primary_key instanceof Double)
		{
			Double s1 = (Double) this.Primary_key;
			Double s2 = (Double) compare.Primary_key;
			return s1.compareTo(s2);
		}
		return 0;
		
	}
	
	@Override
	public boolean equals(Object o)
	{
		Tuple secondTuple = (Tuple) o;
		return this.Primary_key.equals(secondTuple.Primary_key);
	}
}

