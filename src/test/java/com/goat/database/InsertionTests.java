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
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
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
        
		new File("./resources/metadata.csv").delete();
		
		File configFile = new File("resources/DBApp.config");
		Properties props = new Properties();
	    props.setProperty("MaximumRowsCountinPage", "3");
	    FileWriter writer = new FileWriter(configFile);
	    props.store(writer, "");
	    writer.close();

	    
		database = new DBApp();

		
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
	
	// 7ot el rakam w shift el page
	void shiftPage()
	{
		Tuple tmp = page1.tuples.get(page1.tuples.size()-1);
		page1.tuples.remove(page1.tuples.size()-1);
		page2.tuples.insertElementAt(tmp, 0);
	}
	boolean equalTuples(Vector<Tuple> v1, Vector<Tuple> v2)
	{
		if(v1.size()!=v2.size())
			return false;
		for(int i = 0;i<v1.size();i++)
			if(v1.get(i).compareTo(v2.get(i))!=0)
				return false;
		return true;
	}
	// Check that it creates the first page for the first insert
	
	//-----------------------------------------------------TESTS-------------------------------------------------------------------------------------------------
	
	@DisplayName("InsertTable_WithEmptyTable_ShouldCreateFirstPageContainingTuple")
	@Test
	void First_Insert_Is_Succesful() throws DBAppException, IOException, ClassNotFoundException
	{
		colData.put("id", new Integer(1));
		database.insertIntoTable("table", colData);
		assertTrue(new File("./tables/table/table0.ser").exists(),"The serialized page was not found");
		
		Page page = (Page) deserializeData("./tables/table/table0.ser");
		
		tupleForComparing = new Tuple(1,colData);
		assertTrue(page.tuples.get(0).compareTo(tupleForComparing)==0,"The tuple was not inserted correctly");
		
	}
	
	/* Inserting into the middle of a full page should have everything
	 * pushed down; this test checks if all tuples are succesfully stored
	 * with no loss of information
	 */ 	
	@SuppressWarnings("unchecked")
	@DisplayName("InsertTable_InRandomOrder_ShouldHaveThemSortedInPage")
	@Test
	void Insertion_Orders_Tuples() throws DBAppException, IOException, ClassNotFoundException
	{
		Vector<Tuple> firstPageTuples = new Vector<Tuple>(); 
		
		colData.put("id", new Integer(2));
		database.insertIntoTable( "table" , colData );
		firstPageTuples.add(new Tuple(2,colData));
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 8 ));
		database.insertIntoTable( "table" , colData );
		firstPageTuples.add(new Tuple(8,colData));
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 5 ));
		database.insertIntoTable( "table" , colData );
		firstPageTuples.add(new Tuple(5,colData));
		
		Collections.sort(firstPageTuples);
		
		assertTrue(new File("./tables/table/table0.ser").exists(),"The serialized page was not found");
		
		
		Page page = (Page) deserializeData("./tables/table/table0.ser");
		assertTrue(equalTuples(page.tuples,firstPageTuples), "Expected " + firstPageTuples.toString() + " ,but instead got" 
				+ page.tuples);
		
		
	}
	
	// This test checks that if all pages are full, then we create a new page
	// to insert  
	@SuppressWarnings("unchecked")
	@DisplayName("InsertTable_WithFullPage_CreatesNewPageWithTupleInIt")
	@Test
	void Full_Page_Insertion_Leads_To_New_Page_Created() throws DBAppException, IOException, ClassNotFoundException
	{
		Vector<Tuple> firstPageTuples = new Vector<Tuple>(); 
		Vector<Tuple> secondPageTuples = new Vector<Tuple>();
		
		colData.put("id", new Integer(2));
		database.insertIntoTable( "table" , colData );
		firstPageTuples.add(new Tuple(2,colData));
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 5 ));
		database.insertIntoTable( "table" , colData );
		firstPageTuples.add(new Tuple(5,colData));
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 8 ));
		database.insertIntoTable( "table" , colData );
		firstPageTuples.add(new Tuple(8,colData));
				
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 11 ));
		database.insertIntoTable( "table" , colData );
		secondPageTuples.add(new Tuple(11,colData));
		
		
		
		Collections.sort(firstPageTuples);
		
		System.out.println(secondPageTuples.toString());
		assertTrue(new File("./tables/table/table0.ser").exists(),"The serialized page0 was not found");
		assertTrue(new File("./tables/table/table1.ser").exists(),"The serialized page1 was not found");
		
		
		Page page = (Page) deserializeData("./tables/table/table0.ser");
		Page page2 = (Page) deserializeData("./tables/table/table1.ser");
		
		assertTrue(equalTuples(page.tuples,firstPageTuples), "Expected " + firstPageTuples.toString() + " ,but instead got" 
				+ page.tuples);
		assertTrue(equalTuples(page2.tuples,secondPageTuples), "Expected " + secondPageTuples.toString() + " ,but instead got" 
				+ page.tuples);
	}
	
	
	// general case; insert into multiple pages in an unshuffled order
	@SuppressWarnings("unchecked")
	@Test
	@DisplayName("InsertTable_ForMultipleTuples_CreatesPagesAndShiftsTuplesBetweenPagesInOrder")
	void Insertion_For_Multiple_Full_Pages() throws ClassNotFoundException, DBAppException, IOException
	{
		Vector<Tuple> firstPageTuples = new Vector<Tuple>(); 
		Vector<Tuple> secondPageTuples = new Vector<Tuple>();
		Vector<Tuple> thirdPageTuples = new Vector<Tuple>();
		Vector<Tuple> fourthPageTuples = new Vector<Tuple>();
		Vector<Tuple> fifthPageTuples = new Vector<Tuple>();
		
		colData.put("id", new Integer(2));
		database.insertIntoTable( "table" , colData );
		firstPageTuples.add(new Tuple(2,colData));
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 20 ));
		database.insertIntoTable( "table" , colData );
		thirdPageTuples.add(new Tuple(20,colData));
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 7 ));
		database.insertIntoTable( "table" , colData );
		secondPageTuples.add(new Tuple(7,colData));
				
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 11 ));
		database.insertIntoTable( "table" , colData );
		secondPageTuples.add(new Tuple(11,colData));
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 15 ));
		database.insertIntoTable( "table" , colData );
		secondPageTuples.add(new Tuple(15,colData));
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 31 ));
		database.insertIntoTable( "table" , colData );
		fifthPageTuples.add(new Tuple(31,colData));
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 1 ));
		database.insertIntoTable( "table" , colData );
		firstPageTuples.add(new Tuple(1,colData));
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 25 ));
		database.insertIntoTable( "table" , colData );
		fourthPageTuples.add(new Tuple(25,colData));
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 5 ));
		database.insertIntoTable( "table" , colData );
		firstPageTuples.add(new Tuple(5,colData));
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 30 ));
		database.insertIntoTable( "table" , colData );
		fourthPageTuples.add(new Tuple(30,colData));
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 17 ));
		database.insertIntoTable( "table" , colData );
		thirdPageTuples.add(new Tuple(17,colData));
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 19 ));
		database.insertIntoTable( "table" , colData );
		thirdPageTuples.add(new Tuple(19,colData));
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 22 ));
		database.insertIntoTable( "table" , colData );
		fourthPageTuples.add(new Tuple(22,colData));
		
		
		Collections.sort(firstPageTuples);
		Collections.sort(thirdPageTuples);
		Collections.sort(secondPageTuples);
		Collections.sort(fourthPageTuples);
		
		assertTrue(new File("./tables/table/table0.ser").exists(),"The serialized page0 was not found");
		assertTrue(new File("./tables/table/table1.ser").exists(),"The serialized page1 was not found");
		assertTrue(new File("./tables/table/table2.ser").exists(),"The serialized page2 was not found");
		assertTrue(new File("./tables/table/table3.ser").exists(),"The serialized page3 was not found");
		assertTrue(new File("./tables/table/table4.ser").exists(),"The serialized page4 was not found");
		
		
		Page page1 = (Page) deserializeData("./tables/table/table0.ser");
		Page page2 = (Page) deserializeData("./tables/table/table1.ser");
		Page page3 = (Page) deserializeData("./tables/table/table2.ser");
		Page page4 = (Page) deserializeData("./tables/table/table3.ser");
		Page page5 = (Page) deserializeData("./tables/table/table4.ser");

		assertTrue(equalTuples(page1.tuples,firstPageTuples), "Expected " + firstPageTuples.toString() + ", but instead got" 
				+ page1.tuples + " for page " + page1.name);
		assertTrue(equalTuples(page2.tuples,secondPageTuples), "Expected " + secondPageTuples.toString() + ", but instead got" 
				+ page2.tuples + " for page " + page2.name);
		assertTrue(equalTuples(page3.tuples,thirdPageTuples), "Expected " + thirdPageTuples.toString() + ", but instead got" 
				+ page3.tuples + " for page " + page3.name);
		assertTrue(equalTuples(page4.tuples,fourthPageTuples), "Expected " + fourthPageTuples.toString() + ", but instead got" 
				+ page4.tuples + " for page " + page4.name);		
		assertTrue(equalTuples(page5.tuples,fifthPageTuples), "Expected " + fifthPageTuples.toString() + ", but instead got" 
				+ page5.tuples + " for page " + page5.name);
		
	}
	// Checks that if I attempt to insert onto a column that does not exist,
	// an exception is thrown
	@Test
	@DisplayName("InsertTable_WithNonExistentColumnNameInTable_ThrowsException")
	void Exception_Thrown_For_Wrong_Column_Name()
	{
		colData.put("iDoNotExist", new Integer(58));
		colData.put("id", 5);
		Throwable exception =  assertThrows(DBAppException.class, () -> 
		{database.insertIntoTable("table", colData);});	
		
		assertEquals("The hashtable has an extra column that does not exist in the table",exception.getMessage());

	}
	
	// Checks that if I attempt to insert with a wrong datatype for a certain column,
	// an exception is thrown
	@Test
	@DisplayName("InsertTable_WithWrongDataTypeForColumn_ThrowsException")
	void Exception_Thrown_For_Wrong_DataType()
	{
		colData.put("id", new String("wow"));
		assertThrows(DBAppException.class, () -> 
		{database.insertIntoTable("table", colData);});
		colData.clear();
		colData.put("id", new Double(23.4));
		
		Throwable exception = assertThrows(DBAppException.class, () -> 
		{database.insertIntoTable("table", colData);});	
		
		assertEquals("A column was inserted with the wrong datatype",exception.getMessage());
	}
	
	@Test
	@DisplayName("InsertTable_WithWrongDataTypeForColumn_ThrowsException")
	void Exception_Thrown_For_Nonexistent_DataType() throws ClassNotFoundException, DBAppException, IOException
	{
		htbl.put("id", "java.lang.String");
		htbl.put("gpa", "java.lang.Double");
		database.createTable("student", "id", htbl);
		
		colData.put("id",new String("you"));
		colData.put("gpa", new Float(2.3));
		Throwable exception = assertThrows(DBAppException.class, () -> 
		{database.insertIntoTable("student", colData);});
		assertEquals("A column was inserted with the wrong datatype",exception.getMessage());

		colData.clear();
		colData.put("id", new String("wow"));
		colData.put("gpa", new Integer(2));
		exception = assertThrows(DBAppException.class, () -> 
		{database.insertIntoTable("student", colData);});	
		assertEquals("A column was inserted with the wrong datatype",exception.getMessage());
		
		colData.clear();
		colData.put("id", new Integer(23));
		colData.put("gpa", new Double(2.3));
		exception = assertThrows(DBAppException.class, () -> 
		{database.insertIntoTable("student", colData);});	
		assertEquals("A column was inserted with the wrong datatype",exception.getMessage());

	}
	
	@Test
	@DisplayName("InsertTable_WithMissingColumn_ThrowsException")
	void Exception_Thrown_For_Missing_Column() throws ClassNotFoundException, DBAppException, IOException
	{
		htbl.put("age", "java.lang.Integer");
		database.createTable("table2", "id", htbl);
		colData.put("id", new Integer(58));

		Throwable exception = assertThrows(DBAppException.class, () -> 
		{database.insertIntoTable("table2", colData);});
		assertEquals("The hashtable is missing data for one of the columns",exception.getMessage());
	}
	
	@Test
	@DisplayName("InsertTable_WithExtraColumnNotInTable_ThrowsException")
	void ExceptionThrownForExtraColumn()
	{
		colData.put("id", new Integer(5));
		colData.put("extracolumn", new String("where"));
		Throwable exception = assertThrows(DBAppException.class, () -> 
		{database.insertIntoTable("table", colData);});
		
		assertEquals("The hashtable has an extra column that does not exist in the table",exception.getMessage());
	}
	@AfterAll
	static void cleanup() throws IOException
	{
		File configFile = new File("resources/DBApp.config");
		Properties props = new Properties();
	    props.setProperty("MaximumRowsCountinPage", "4");
	    FileWriter writer = new FileWriter(configFile);
	    props.store(writer,"");
	    writer.close();
	}
	
}
