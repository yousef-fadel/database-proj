package com.goat.database;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

public class HelperMethods {
	
	// TODO move to main class
	// given a table name, primary key, and information about the table columns, it writes onto the csv file 
	// all info about this table
	public void writeCSV(String strTableName, String strClusteringKeyColumn,Hashtable<String,String> htblColNameType) throws IOException
	{
		File file = new File("./resources/metadata.csv"); 
	    try { 
	        FileWriter outputfile = new FileWriter("./resources/metadata.csv",true); 
	        CSVWriter writer = new CSVWriter(outputfile);
	        Set<String> setOfKeys = htblColNameType.keySet();
	        for(String keys : setOfKeys)
	        {
	        	if(strClusteringKeyColumn.equals((String)keys))
	        	{
	        		String[] header = {strTableName, keys, htblColNameType.get(keys), "True", "null", "null"};
	        		writer.writeNext(header);
	        	}
	        	else
	        	{
	        		String[] header = {strTableName, keys, htblColNameType.get(keys), "False", "null", "null"};
	        		writer.writeNext(header);
	        	}

	        }
	 	    writer.close(); 
	    } 
	    catch (IOException e) { 
	        e.printStackTrace(); 
	    }	
	}
	
	//given a table name, returns a 2D list containing all information about its columns
	public List<List<String>> getColumnData(String tableName) throws IOException
	{
		List<List<String>> records = new ArrayList<List<String>>();
		try (CSVReader csvReader = new CSVReader(new FileReader("./resources/metadata.csv"));) {
		    String[] values = null;
		    while ((values = csvReader.readNext()) != null && values[0].equals(tableName)) {
		        records.add(Arrays.asList(values));
		    }
		}
		return records;
	}
	
}
