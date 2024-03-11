package com.goat.database;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class Page implements java.io.Serializable
{
	String name;
	int num;//number starts from 0
	String pageFilepath; //taken from table
	Vector<Tuple> tuples = new Vector<Tuple>();
	public int maxNoEnteries;//get maxNoEnteries from file
	
	public Page(String name, int num, String tableFilePath) throws DBAppException
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
        this.name = name;
        this.num = num;
        this.pageFilepath = tableFilePath + name + ".ser/";
	}
	
	// give ONLY the table.filepath, the method will add the rest
	public void serializePage()
	{
		try {
			FileOutputStream file = new FileOutputStream(pageFilepath);
			ObjectOutputStream out = new ObjectOutputStream(file);
			out.writeObject(this);
			out.close();
			file.close();
		} catch (IOException e) {
			System.out.println("Failed to serialize page!");
			e.printStackTrace();
		}
	}
	
	public Page serializeAndDeletePage()
	{
		serializePage();
		return null;
	}
	
	
}
