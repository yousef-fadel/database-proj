import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Set;

import com.opencsv.CSVWriter;

public class HelperMethods {
	public void writeCSV(String strTableName, String strClusteringKeyColumn,Hashtable<String,String> htblColNameType) throws IOException
	{
		File file = new File("metadata.csv"); 
	    try { 
	        FileWriter outputfile = new FileWriter("metadata.csv"); 
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
	        // TODO Auto-generated catch block 
	        e.printStackTrace(); 
	    }	
	}
}
