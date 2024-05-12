package com.goat.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.opencsv.CSVReader; 

public class CreateTableTests {
	 
	DBApp database;
	Vector<Table> tables;
	Table table1;
	Table table2;
	Hashtable<String,String> htbl;
	
	// setup creates 2 tables and saves them onto the ser file
	// also creates a hashtable for calling the create table method
	// running tests will delete the tables.ser file tho everytime
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
		new File("./tables/table0").mkdirs();
		new File("./tables/table1").mkdirs();
		
		htbl = new Hashtable<String,String>();
		htbl.put("id", "java.lang.Integer");
		
		table1 = new Table("table0","./tables/table0/");
		table2 = new Table("table1","./tables/table1/");
		tables = new Vector<Table>();
		
		tables.add(table1);
		tables.add(table2);
		
		table1.serializeTable();
		table2.serializeTable();
		
		database = new DBApp();
		
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
//-------------------------------------------------------TESTS--------------------------------------------------
	
	
	// If there are tables already saved on the hard disk, then the method should 
	// find them and insert them onto our vector of tables
	@Test
	@DisplayName("CreateTable_WithPreExistingTables_ShouldFindThem")
	void Existing_Tables_Should_Be_Found() throws IOException, ClassNotFoundException, DBAppException
	{

		database.createTable("table6", "id", htbl);
		assertTrue(database.tables.size()==3);
		assertTrue(database.tables.get(0).name.equals(tables.get(0).name) || database.tables.get(0).name.equals(tables.get(1).name) );
		assertTrue(database.tables.get(1).name.equals(tables.get(0).name) || database.tables.get(1).name.equals(tables.get(1).name) );
	}
	
	//If a table with the same already exists, an exception should be thrown
	@Test
	@DisplayName("CreatingTwoTables_WithSameName_ShouldThrowException")
	public void Throw_Exception_For_Two_Tables_With_Same_Name()
	{
		
		Throwable exception = assertThrows(DBAppException.class, () -> 
		{database.createTable("table1", "id", htbl);});
		
		assertEquals("A table of this name already exists",exception.getMessage());
	}
	
	// After creating a table, there should be an empty directory 
	// in the folder "tables" to store its pages
	@Test
	@DisplayName("CreateTable_Normally_ShouldFindDirectoryForThatTable")
	public void Should_Find_Directory_For_Table() throws ClassNotFoundException, DBAppException, IOException
	{
		database.createTable("newtable", "id", htbl);
		File directory = new File("./tables/newtable");
		assertTrue(directory.exists());
		
	}
	
	// After creating multiple tables, we should be able to fetch them from the
	// hard disk at any time
	@Test
	@DisplayName("CreateTable_CreatingMultipleTable_ShouldFindAllTableSerializedInFolders")
	public void Deserializating_Tables_Should_Contain_All_Tables() throws ClassNotFoundException, DBAppException, IOException
	{
		String names[] = {"table0","table1","table2","table3","table4"};
		database.createTable(names[2], "id", htbl);
		database.createTable(names[3], "id", htbl);
		database.createTable(names[4], "id", htbl);
		File file = new File("./tables");
		String[] fileNames = file.list();
		for(String name : fileNames)
		{
		    if (new File("./tables/" + name).isDirectory())
		    {
		        tables.add((Table) deserializeData("./tables/"+name+ "/info.ser"));
		    }
		}
		ArrayList<String> tableNamesDeserialized = new ArrayList<String>();
		for(int i =0;i<tables.size();i++)
		{
			tableNamesDeserialized.add(tables.get(i).name);
		}
		for(int i =0;i<names.length;i++)
		{
			assertTrue(tableNamesDeserialized.contains(names[i]));
		}
	}
	
	// Creating tables should have the CSV file containing all info about it
	@Test
	@DisplayName("CreateTable_Normally_ShouldUpdateMetaDataFileToContainTableInfo")
	public void CSV_File_Should_Contain_Column_Data() throws ClassNotFoundException, DBAppException, IOException
	{
		htbl.put("age", "java.lang.Double");
		
		Hashtable<String,String> htbl2 = new Hashtable<String, String>();
		
		htbl2.put("name", "java.lang.String");
		htbl2.put("yee", "java.lang.Integer");
		
		database.createTable("greattable", "id", htbl);
		database.createTable("bettertable", "name", htbl2);
		
		CSVReader reader = new CSVReader(new FileReader("./resources/metadata.csv"));
		ArrayList<String[]> csvFile = new ArrayList<>();
        String[] nextRow;
        while ((nextRow = reader.readNext()) != null) 
        	csvFile.add(nextRow);
        reader.close();
        // or CSVReader reader = new CSVReader(new FileReader(csvFile)); 
        // List<String[]> rows = reader.readAll();

        String[] greatrow1 = {"greattable","id","java.lang.Integer","True","null","null"};
        String[] greatrow2 = {"greattable","age","java.lang.Double","False","null","null"};
        String[] betterrow1 = {"bettertable","name","java.lang.String","True","null","null"};
        String[] betterrow2 = {"bettertable","yee","java.lang.Integer","False","null","null"};

        for (String[] arr : csvFile) {
            assertTrue(Arrays.equals(arr, greatrow1) || Arrays.equals(arr, greatrow2) ||
            		Arrays.equals(arr, betterrow1) || Arrays.equals(arr, betterrow2));
        }
	}
	
	// Creating a table with a datatype other than int, double, and string should
	// throw an exception
	//TODO check exception message
	@Test
	@DisplayName("CreateTable_WithWrongDataType_ShouldThrowException")
	public void Only_Certain_Datatypes_Allowed()
	{
		htbl.put("invalidcolumn", "java.lang.Float");
		Throwable exception = assertThrows(DBAppException.class, () -> 
		{database.createTable("table", "id", htbl);});	
		
		assertEquals("Column has invalid datatype",exception.getMessage());
	}
	
	// Having the clustering key be a column that does not exist in the hashtable
	// should throw an exception
	// TODO check exception message
	@Test
	@DisplayName("CreateTable_WithClusteringKeyMissing_ShouldThrowException")
	public void Primary_Key_Should_Exist_In_HashTable()
	{
		//only column in our htbl is id
		Throwable exception = assertThrows(DBAppException.class, () -> 
		{database.createTable("table", "anaMeshMawgood", htbl);});	

		assertEquals("Clustering key does not exist in columns",exception.getMessage());

	}

	@AfterAll
	static void cleanup() throws IOException
	{
		new File("./resources/metadata.csv").delete();
		File configFile = new File("resources/DBApp.config");
		Properties props = new Properties();
	    props.setProperty("MaximumRowsCountinPage", "4");
	    FileWriter writer = new FileWriter(configFile);
	    props.store(writer,"");
	    writer.close();
	}

}
