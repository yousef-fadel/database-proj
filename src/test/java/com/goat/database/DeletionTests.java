package com.goat.database;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
@SuppressWarnings("removal")
public class DeletionTests {

	DBApp database;
	Hashtable<String,String> htbl;
	Hashtable<String,Object> colData;
	Table table;
	Tuple tupleForComparing;
	Page page1;
	Page page2;
	Page page3;

	@BeforeEach
	void init() throws IOException, ClassNotFoundException, DBAppException
	{
		Path dir = Paths.get("./tables"); 
        Files
            .walk(dir)
            .sorted(Comparator.reverseOrder())
            .forEach(path -> {
                 try {Files.delete(path);} 
                 catch (IOException e) {e.printStackTrace();}});
        
		new File("./resources/metadata.csv").delete();

		File configFile = new File("resources/DBApp.config");
		Properties props = new Properties();
	    props.setProperty("MaximumRowsCountinPage", "3");
	    FileWriter writer = new FileWriter(configFile);
	    props.store(writer, "");
	    writer.close();
	    
		database = new DBApp();

		page1 = new Page("table0.ser",0,"tables/table/");
		page2 = new Page("table1.ser",1,"tables/table/");
		page3 = new Page("table2.ser",2,"tables/table/");

		htbl = new Hashtable<String,String>();
		htbl.put("id", "java.lang.Integer");
		database.createTable("table", "id", htbl);

		colData = new Hashtable<String, Object>();
		table = database.tables.get(0);
		
	}
	public void serializedata(Object o, String filename) throws IOException 
	{
		FileOutputStream file = new FileOutputStream(filename);
		ObjectOutputStream out = new ObjectOutputStream(file);
		out.writeObject(o);
		out.close();
		file.close();
	}
	public Object deserializeData(String filename) throws ClassNotFoundException, IOException 
	{
		FileInputStream fileIn = new FileInputStream(filename);
		ObjectInputStream in = new ObjectInputStream(fileIn);
		Object output =in.readObject();
		in.close();
		fileIn.close();
		return output;
	}
	
	//-------------------------------------------------------TESTS--------------------------------------------------------------
	//This checks if one tuple is deleted from the page
	@Test
	void Delete_Tuple_From_Page() throws IOException, DBAppException, ClassNotFoundException
	{
		colData.put("id", new Integer(5));
		Tuple tuple = new Tuple(5,colData);

		colData = new Hashtable<String, Object>();
		colData.put("id", new Integer(10));
		Tuple tobedeleted = new Tuple(6,colData);

		page1.tuples.add(tuple);
		page1.tuples.add(tobedeleted);
		table.pageNames.add(page1.name);
		table.numberForPage = 1;
		
		page1.serializePage();
		database.deleteFromTable("table", colData);
		
		page1 = (Page) deserializeData(page1.pageFilepath);
		System.out.println(page1.tuples.toString());
		assertTrue(page1.tuples.size()==1, "Expected one tuple to be in the page, but instead there are "
				+ page1.tuples.size() + " tuples in the page");
		assertTrue(page1.tuples.get(0).compareTo(tuple)==0, "Expected tuple " + tuple + ", but instead got tuple " + page1.tuples.get(0));
	}

	// This checks that if multiple tuples satisfy the deletion condition,
	// all of them are also deleted
//	@Test
	void Delete_Multiple_Tuples_From_Page()
	{

	}

	//This checks that no gaps are left when deleting from a page
	@Test
	void No_Empty_Space_In_Page_After_Deletion() throws DBAppException, ClassNotFoundException, IOException
	{
		colData.put("id", new Integer(2));
		Tuple tuple1 = new Tuple(2,colData);
		
		colData = new Hashtable<String, Object>();
		colData.put("id", new Integer(5));
		Tuple tobedeleted = new Tuple(5,colData);

		colData = new Hashtable<String, Object>();
		colData.put("id", new Integer(10));
		Tuple tuple2 = new Tuple(6,colData);

		page1.tuples.add(tuple1);
		page1.tuples.add(tobedeleted);
		page1.tuples.add(tuple2);
		table.pageNames.add(page1.name);
		table.numberForPage = 1;
		
		page1.serializePage();
		
		colData = new Hashtable<String, Object>();
		colData.put("id", new Integer(5));
		database.deleteFromTable("table", colData);
		
		page1 = (Page) deserializeData(page1.pageFilepath);
		
		assertTrue(page1.tuples.size()==2, "Expected number of tuples in page to be two, but instead there are "
				+ "" + page1.tuples.size() + " tuples in the page");
		assertTrue(page1.tuples.get(0).compareTo(tuple1)==0, "Expected first tuple in page to be " + tuple1 + " but instead got "
				   + page1.tuples.get(0));		
		assertTrue(page1.tuples.get(1).compareTo(tuple2)==0, "Expected second tuple in page to be " + tuple2 + " but instead got "
				+ page1.tuples.get(1));
		
	}

	// This checks that if we delete the last tuple from a page, it
	// also deletes the entire page
	@Test
	void Delete_Page_Once_Last_Tuple_Is_Deleted() throws DBAppException, IOException
	{
		colData.put("id", new Integer(5));
		Tuple tuple = new Tuple(5,colData);

		page1.tuples.add(tuple);
		table.pageNames.add(page1.name);
		table.numberForPage = 1;
		
		page1.serializePage();

		database.deleteFromTable("table", colData);
		
		assertTrue(table.pageNames.size()==0, "Page name was not deleted from the table");
		
		assertThrows(FileNotFoundException.class, () ->
		{deserializeData(page1.pageFilepath);}, "Page was not deleted from the hard disk");
	}

	// This checks that deleting the tuple also deletes it from the
	// index (if it exists)
	@Test 
	@Disabled
	void Delete_Tuple_From_Index_Too()
	{

	}
}
