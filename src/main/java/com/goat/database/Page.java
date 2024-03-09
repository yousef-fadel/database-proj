package com.goat.database;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class Page implements java.io.Serializable
{
	String name;
	Vector<Tuple> tuples = new Vector<Tuple>();
	public int maxNoEnteries;//get maxNoEnteries from file
	
	
	public Page() throws DBAppException
	{
        try {
            String configFilePath = "resources/DBApp.config";
            FileInputStream propsInput = new FileInputStream(configFilePath);
            Properties prop = new Properties();
            prop.load(propsInput);
            maxNoEnteries = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
            
        } catch (FileNotFoundException e) {
            throw new DBAppException("DBApp.config file was not found.");
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	public static void main(String[]args)
	{
//		Page page = new Page();
//		System.out.println(page.maxNoEnteries);
	}
	
}
