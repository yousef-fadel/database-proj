	package com.goat.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
@SuppressWarnings("removal")
public class InsertionTests {
	// all tests here work on the assumption that createTable is working
	DBApp database;
	Hashtable<String,String> htbl;
	Hashtable<String,Object> colData;
	Table table;
	Tuple tupleForComparing;
	Page page1;
	Page page2;
	
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
        
		new File("./resources/tables.ser").delete();
		new File("./resources/metadata.csv").delete();
		
//		File configFile = new File("resources/DBApp.config");
//		Properties props = new Properties();
//	    props.setProperty("MaximumRowsCountinPage", "4");
//	    FileWriter writer = new FileWriter(configFile);
//	    props.store(writer, "");
//	    writer.close();

		database = new DBApp();

		page1 = new Page();
		page2 = new Page();
		
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
	
	
	// fills the page; count is used as the starting point, and fillCompletely fills the page completely if it is true
	// is used to fill the page completely if needed 
	void fillPage(int count,boolean fillCompletely, Page page) throws DBAppException
	{
		for(int i = count;i<count+(page.maxNoEnteries/2);i++)
		{
			Hashtable<String,Object> tmp = new Hashtable<String, Object>();
			tmp.put("id", new Integer(i));
			page.tuples.add(new Tuple(i,tmp));
		}
		for(int i = 1000 + count;i<1000 + count + (page.maxNoEnteries/2 + page.maxNoEnteries%2) 
				&& fillCompletely;i++)
		{
			Hashtable<String,Object> tmp = new Hashtable<String, Object>();
			tmp.put("id", new Integer(i));
			page.tuples.add(new Tuple(i,tmp));
		}

		
	}
	
	//TODO generalize this method
	// 7ot el rakam w shift el page
	void shiftPage(Hashtable<String,Object> htbl)
	{
		Tuple tmp = page1.tuples.get(page1.tuples.size()-1);
		for(int i = page1.tuples.size()-1;i > 100;i--)
		{
			page1.tuples.set(i, page1.tuples.get(i-1));
		}
		page1.tuples.set(100, new Tuple(150,htbl));
		
		Tuple tmp2 = page2.tuples.get(page2.tuples.size()-1);
		for(int i = page2.tuples.size()-1;i > 0;i--)
		{
			page2.tuples.set(i, page2.tuples.get(i-1));
		}
		page2.tuples.set(0, tmp);
		page2.tuples.add(tmp2);
	}
	// Check that it creates the first page for the first insert
	
	//-----------------------------------------------------TESTS-------------------------------------------------------------------------------------------------
	
	@DisplayName("Inserting into an empty table")
	@Test
	void First_Insert_Is_Succesful() throws DBAppException, IOException, ClassNotFoundException
	{
		colData.put("id", new Integer(1));
		database.insertIntoTable("table", colData);
		assertTrue(new File("./tables/table/table1.ser").exists(),"The serialized page was not found");
		
		Page page = (Page) deserializeData("./tables/table/table1.ser");
		
		tupleForComparing = new Tuple(1,colData);
		assertTrue(page.tuples.get(0).equals(tupleForComparing),"The tuple was not inserted correctly");
		
	}
	
	/* Inserting into the middle of a full page should have everything
	 * pushed down; this test checks if all tuples are succesfully stored
	 * with no loss of information
	 */ 	
	@DisplayName("Inserting into a full page shifts all tuples down")
	@Test
	@Timeout(value = 200)
	void Insertion_Shifts_All_Tuples() throws DBAppException, IOException, ClassNotFoundException
	{
		fillPage(1,true,page1);
		fillPage(1200,false,page2);
		
		page1.name = "table1.ser";
		page2.name = "table2.ser";
		
		table.pageNames.add(page1.name);
		table.pageNames.add(page2.name);
		
		serializedata(page1, table.filepath+page1.name);
		serializedata(page2, table.filepath+page2.name);
		
		colData.put("id", new Integer(150));
		database.insertIntoTable("table", colData);
		
		shiftPage(colData);
		
		Page deserializedPage1 = (Page)deserializeData(table.filepath+page1.name);
		Page deserializedPage2 = (Page)deserializeData(table.filepath+page2.name);
		
		assertEquals(page1.tuples,deserializedPage1.tuples);
		assertEquals(page2.tuples,deserializedPage2.tuples);
	}
	
	// This test checks that if all pages are full, then we create a new page
	// to insert  
	@DisplayName("Inserting into full pages leads to a new page being created")
	@Test
	void Full_Page_Insertion_Leads_To_New_Page_Created() throws DBAppException, IOException, ClassNotFoundException
	{
		fillPage(1,true,page1);
		fillPage(1200,true,page2);
		page1.name = "table1.ser";
		page2.name = "table2.ser";
		
		table.pageNames.add(page1.name);
		table.pageNames.add(page2.name);
		
		serializedata(page1, table.filepath+page1.name);
		serializedata(page2, table.filepath+page2.name);
		
		colData.put("id", new Integer(150));
		database.insertIntoTable("table", colData);
		
		Page deserializedPage3 = (Page)deserializeData(table.filepath+table.pageNames.get(2));
		tupleForComparing = page2.tuples.get(page2.tuples.size()-1);
		
		assertTrue(table.pageNames.size()==3, "A new page was not created or the name was not added to the table itself");
		assertTrue(deserializedPage3.tuples.get(0).equals(tupleForComparing),"The last element of the last page was expected, but it was not found in the last page");
	}
	
	//TODO This test checks that if we insert onto a table with an indexed column,
	// it inserts into that index too
	
	@Test
	@Disabled("not implemented")
	void Insertion_With_Index()
	{
		
	}
	
	//TODO same as insertino for full pages, but for even more pages
	@Test
	@Disabled
	void Insertion_For_Multiple_Full_Pages()
	{
		
	}
	// Checks that if I attempt to insert onto a column that does not exist,
	// an exception is thrown
	@Test
	@DisplayName("Exception is thrown for having a wrong column name")
	void Exception_Thrown_For_Wrong_Column_Name()
	{
		colData.put("iDoNotExist", new Integer(58));
		assertThrows(DBAppException.class, () -> 
		{database.insertIntoTable("table", colData);});	
	}
	
	// Checks that if I attempt to insert with a wrong datatype for a certain column,
	// an exception is thrown
	@Test
	@DisplayName("Exception is thrown for having wrong datatype")
	void Exception_Thrown_For_Wrong_DataType()
	{
		colData.put("id", new String("wow"));
		assertThrows(DBAppException.class, () -> 
		{database.insertIntoTable("table", colData);});
		colData.clear();
		colData.put("id", new Double(23.4));
		assertThrows(DBAppException.class, () -> 
		{database.insertIntoTable("table", colData);});	
	}
	
	// Check that inserting without a primary key throws an exception
	@Test
	@DisplayName("Exception is thrown for a missing primary key")
	void Exception_Thrown_For_No_Primary_Key() throws ClassNotFoundException, DBAppException, IOException
	{
		htbl.put("age", "java.lang.Integer");
		database.createTable("table2", "id", htbl);
		colData.put("age", new Integer(58));
		assertThrows(DBAppException.class, () -> 
		{database.insertIntoTable("table2", colData);});	
	}
	
	@Test
	@DisplayName("Exception is thrown for having a missing column")
	void Exception_Thrown_For_Missing_Column() throws ClassNotFoundException, DBAppException, IOException
	{
		htbl.put("age", "java.lang.Integer");
		database.createTable("table2", "id", htbl);
		colData.put("id", new Integer(58));
		assertThrows(DBAppException.class, () -> 
		{database.insertIntoTable("table2", colData);});	
	}
	
	@AfterAll
	static void cleanup() throws IOException
	{
		File configFile = new File("resources/DBApp.config");
		Properties props = new Properties();
	    props.setProperty("MaximumRowsCountinPage", "200");
	    FileWriter writer = new FileWriter(configFile);
	    props.store(writer,"");
	    writer.close();
	}
	
}
