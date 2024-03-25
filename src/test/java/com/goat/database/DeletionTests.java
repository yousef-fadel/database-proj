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
	// TODO test exceptions (5odo copy paste mein insertion tests)
	DBApp database;
	Hashtable<String,String> htbl;
	Hashtable<String,Object> colData;
	Table result; // use this to store the result we want (do it by comparing)
	Table banadyMethod; // call the methods on this table
	Tuple tupleForComparing;
	Page deserializedPage; // 
	Page pageAfterDeletion; // how the table should look like after deletion
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

//		page1 = new Page("table0.ser",0,"tables/table/");
//		page2 = new Page("table1.ser",1,"tables/table/");
//		page3 = new Page("table2.ser",2,"tables/table/");

		htbl = new Hashtable<String,String>();
		htbl.put("id", "java.lang.Integer");
		
		database.createTable("banadyMethod", "id", htbl);
		database.createTable("result", "id", htbl);
		
		
		colData = new Hashtable<String, Object>();
		banadyMethod = database.tables.get(0);
		result = database.tables.get(1);

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
		try {
			FileInputStream fileIn;
			if(filename.contains(".ser"))
				fileIn = new FileInputStream(filename);
			else
				fileIn = new FileInputStream(filename + ".ser"); 
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Object output = in.readObject();
			in.close();
			fileIn.close();
			return output;
		}
		catch(IOException e) 
		{
			e.printStackTrace();
		}
		return null;
	}

	//-------------------------------------------------------TESTS--------------------------------------------------------------
	//This checks if one tuple is deleted from the page
	@Test
	void Delete_Tuple_From_Page() throws IOException, DBAppException, ClassNotFoundException
	{
		colData.put("id", new Integer(5));
		database.insertIntoTable("result", colData);
		database.insertIntoTable("banadyMethod", colData);
		
		colData.clear();
		colData.put("id", new Integer(10));
		database.insertIntoTable("banadyMethod", colData);

		database.deleteFromTable("banadyMethod", colData);

		Page banadyMethodPage = (Page) deserializeData(banadyMethod.filepath + banadyMethod.name + 0);
		Page resultPage = (Page) deserializeData(result.filepath + result.name + 0);
		Tuple expectedTuple = resultPage.tuples.get(0);
		
		assertTrue(banadyMethodPage.tuples.size()==1, "Expected one tuple to be in the page, but instead there are "
				+ banadyMethodPage.tuples.size() + " tuples in the page");
		assertTrue(banadyMethodPage.tuples.get(0).compareTo(expectedTuple)==0, 
				"Expected tuple " + expectedTuple + ", but instead got tuple " + banadyMethodPage.tuples.get(0));
	}

	// Checks that if no tuples meet condition for deleted, then nothing is deleted
	@Test
	@Disabled
	void No_Tuples_Deleted_For_False_Info()
	{

	}

	//This checks that no gaps are left when deleting from a page
	@Test
	void No_Empty_Space_In_Page_After_Deletion() throws DBAppException, ClassNotFoundException, IOException
	{
		colData.put("id", new Integer(2));
		database.insertIntoTable("result", colData);
		database.insertIntoTable("banadyMethod", colData);

		colData.clear();
		colData = new Hashtable<String, Object>();
		colData.put("id", new Integer(5));
		database.insertIntoTable("banadyMethod", colData);

		colData.clear();
		colData = new Hashtable<String, Object>();
		colData.put("id", new Integer(10));
		database.insertIntoTable("result", colData);
		database.insertIntoTable("banadyMethod", colData);

		colData = new Hashtable<String, Object>();
		colData.put("id", new Integer(5));
		database.deleteFromTable("table", colData);

		Page banadyMethodPage = (Page) deserializeData(banadyMethod.filepath + banadyMethod.name + 0);
		Page resultPage = (Page) deserializeData(result.filepath + result.name + 0);

		assertTrue(banadyMethodPage.tuples.size()==2, "Expected number of tuples in page to be two, but instead there are "
				+ "" + banadyMethodPage.tuples.size() + " tuples in the page");
		assertTrue(banadyMethodPage.tuples.get(0).compareTo(resultPage.tuples.get(0))==0, 
				"Expected first tuple in page to be " + resultPage.tuples.get(0) + " but instead got "
				+ banadyMethodPage.tuples.get(0));		
		assertTrue(banadyMethodPage.tuples.get(1).compareTo(resultPage.tuples.get(1))==0, 
				"Expected second tuple in page to be " + resultPage.tuples.get(1) + " but instead got "
				+ banadyMethodPage.tuples.get(1));

	}

	// This checks that if we delete the last tuple from a page, it
	// also deletes the entire page
	@Test
	void Delete_Page_Once_Last_Tuple_Is_Deleted() throws DBAppException, IOException, ClassNotFoundException
	{
		colData.put("id", new Integer(5));
		database.insertIntoTable("banadyMethod", colData);	

		database.deleteFromTable("table", colData);

		assertTrue(banadyMethod.pageNames.size()==0, "Page name was not deleted from the table");

		assertThrows(FileNotFoundException.class, () ->
		{deserializeData(banadyMethod.filepath + "banadyMethod0");}, "Page was not deleted from the hard disk");
	}

	// This checks that if multiple tuples satisfy the deletion condition,
	// all of them are also deleted
	@Disabled
	@Test
	void Delete_Multiple_Tuples_From_Page()
	{

	}

	// This checks that if I have multiple deletion conditions,
	// all of tuples meeting that condition are deleted
	@Test
	@Disabled
	void Delete_Multiple_Tuple_With_Multiple_Deletion_Conditions()
	{

	}

	// This checks that deleting the tuple also deletes it from the
	// index (if it exists)
	@Test 
	@Disabled
	void Delete_Tuple_From_Index_Too()
	{

	}
}
