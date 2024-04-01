package com.goat.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
public class IndexTests {
	// test 2: create index for a table with multiple rows (w/duplicates) -- how 
	
	
	// all tests here work on the assumption that createTable and insertTable are working fine
	DBApp database;
	Hashtable<String,String> htbl;
	Hashtable<String,Object> colData;
	Table table;
	int pageSize;
	Random random;
	DecimalFormat df;
	
	@BeforeEach
	void init() throws IOException, ClassNotFoundException, DBAppException
	{
		pageSize = 3;
		random = new Random();
		df = new DecimalFormat("#.####");
		df.setRoundingMode(RoundingMode.CEILING);
		
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
	    props.setProperty("MaximumRowsCountinPage", pageSize+"");
	    FileWriter writer = new FileWriter(configFile);
	    props.store(writer, "");
	    writer.close();

	    
		database = new DBApp();

		
		htbl = new Hashtable<String,String>();
		htbl.put("id", "java.lang.Integer");
		htbl.put("age", "java.lang.Integer");
		htbl.put("name", "java.lang.String");
		htbl.put("gpa", "java.lang.Double");
		database.createTable("table", "id", htbl);
		
		colData = new Hashtable<String, Object>();
		table = database.tables.get(0);
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
	
	private String randomString()
	{
		String res = "";
		int pickLetter;
		char[] characters ={'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'};
		for(int i = 0;i<10;i++)
		{
			pickLetter = random.nextInt(52);
			res += characters[pickLetter];
		}
		return res;
	}
	//---------------------------------------------------------TESTS-------------------------------------------------------
	
	@Test
	public void createIndexForTableWithNoRowsInteger() throws DBAppException, IOException, ClassNotFoundException
	{
		database.createIndex("table","id","idIndex");
		List<List<String>> colDataTypes = database.getColumnData("table");
		for (int i = 0; i < colDataTypes.size(); i++)
			if (colDataTypes.get(i).get(1).equals("id"))
			{
				assertTrue(colDataTypes.get(i).get(4).equals("idIndex"), "The index name in the CSV file was not changed");
				assertTrue(colDataTypes.get(i).get(5).equals("B+tree"), "The index type in the CSV file was not changed");
				File directory = new File("./tables/table/indices/idIndex.ser");
				assertTrue(directory.exists(),"Could not find index file");
				return;
			}
		assertTrue(false, "Could not find the column ID in the metadata file");
		
		
	}
	
	@Test
	public void createIndexForTableWithNoRowsDouble() throws IOException, DBAppException, ClassNotFoundException
	{
		database.createIndex("table","gpa","gpaIndex");
		List<List<String>> colDataTypes = database.getColumnData("table");
		for (int i = 0; i < colDataTypes.size(); i++)
			if (colDataTypes.get(i).get(1).equals("gpa"))
			{
				assertTrue(colDataTypes.get(i).get(4).equals("gpaIndex"), "The index name in the CSV file was not changed");
				assertTrue(colDataTypes.get(i).get(5).equals("B+tree"), "The index type in the CSV file was not changed");
				File directory = new File("./tables/table/indices/gpaIndex.ser");
				assertTrue(directory.exists(),"Could not find index file");
				return;
			}
		assertTrue(false, "Could not find the column ID in the metadata file");
	}
	
	@Test
	public void createIndexForTableWithNoRowsString() throws DBAppException, IOException, ClassNotFoundException
	{
		database.createIndex("table","name","nameIndex");
		List<List<String>> colDataTypes = database.getColumnData("table");
		for (int i = 0; i < colDataTypes.size(); i++)
			if (colDataTypes.get(i).get(1).equals("name"))
			{
				assertTrue(colDataTypes.get(i).get(4).equals("nameIndex"), "The index name in the CSV file was not changed");
				assertTrue(colDataTypes.get(i).get(5).equals("B+tree"), "The index type in the CSV file was not changed");
				File directory = new File("./tables/table/indices/nameIndex.ser");
				assertTrue(directory.exists(),"Could not find index file");
				return;
			}
		assertTrue(false, "Could not find the column ID in the metadata file");
	}
	
	@Test
	public void createIndexForTableWithMultipleRowsInteger() throws DBAppException, ClassNotFoundException, IOException
	{
		int	noOfPages = pageSize*6;
		int [] uniqueAge = random.ints(0,500).distinct().limit(pageSize*7).toArray();
		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 1000) / 1000)
                .mapToDouble(Double::doubleValue).limit(pageSize*7).toArray();
		for(int i = 0;i<noOfPages;i++)
		{
			int age =  uniqueAge[i];
			double gpa = uniqueGPA[i];
			String name = randomString();
			colData.clear();
			colData.put("id", age);
			colData.put("age", age);
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("table", colData);
		}
		database.createIndex("table", "age", "ageIndex");
		
		Index resultIndex = (Index) deserializeData(table.filepath+"/indices/" + "ageIndex.ser");
		int NumberIndexedSearchResult = resultIndex.searchGreaterThan(new Datatype(0), true).size();
		assertTrue(NumberIndexedSearchResult == noOfPages,
				"Expected the number of pointers to be "+ noOfPages+ ", but instead got "+ NumberIndexedSearchResult);
	}
	
	@Test
	public void createIndexForTableWithMultipleRowsString() throws DBAppException, ClassNotFoundException, IOException
	{
		int	noOfPages = pageSize*6;
		int [] uniqueAge = random.ints(0,500).distinct().limit(pageSize*7).toArray();
		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 1000) / 1000)
				.mapToDouble(Double::doubleValue).limit(pageSize*7).toArray();
		for(int i = 0;i<noOfPages;i++)
		{
			int age =  uniqueAge[i];
			double gpa = uniqueGPA[i];
			String name = randomString();
			colData.clear();
			colData.put("id", age);
			colData.put("age", age);
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("table", colData);
		}
		database.createIndex("table", "gpa", "gpaIndex");

		Index resultIndex = (Index) deserializeData(table.filepath+"/indices/" + "gpaIndex.ser");
		int NumberIndexedSearchResult = resultIndex.searchGreaterThan(new Datatype(0.0), true).size();
		assertTrue(NumberIndexedSearchResult == noOfPages,
				"Expected the number of pointers to be "+ noOfPages+ ", but instead got "+ NumberIndexedSearchResult);
	}
	
	@Test
	public void createIndexForTableWithMultipleRowsDouble() throws DBAppException, ClassNotFoundException, IOException
	{
		int	noOfPages = pageSize*6;

		int [] uniqueAge = random.ints(0,500).distinct().limit(pageSize*7).toArray();
		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 1000) / 1000)
				.mapToDouble(Double::doubleValue).limit(pageSize*7).toArray();
		for(int i = 0;i<noOfPages;i++)
		{
			int age =  uniqueAge[i];
			double gpa = uniqueGPA[i];
			String name = randomString();
			colData.clear();
			colData.put("id", age);
			colData.put("age", age);
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("table", colData);
		}
		database.createIndex("table", "name", "nameIndex");
		
		Index resultIndex = (Index) deserializeData(table.filepath+"/indices/" + "nameIndex.ser");
		int NumberIndexedSearchResult = resultIndex.searchGreaterThan(new Datatype(""), true).size();
		assertTrue(NumberIndexedSearchResult == noOfPages,
				"Expected the number of pointers to be "+ noOfPages+ ", but instead got "+ NumberIndexedSearchResult);
	}
	
	@Test
	public void throwExceptionForNonExistentColumnName()
	{
		Throwable exception =  assertThrows(DBAppException.class, () -> 
		{database.createIndex("table", "yaretnyMawgoodo", "0b");});	
		
		assertEquals("Column name not found",exception.getMessage());
	}
	
	@Test
	public void throwExceptionForNonExistentTableName()
	{
		Throwable exception =  assertThrows(DBAppException.class, () -> 
		{database.createIndex("pspspspspsppspspsp", "id", "id");});	
		
		assertEquals("Table does not exist",exception.getMessage());
	}
	
	@Test
	// attempting to create an index for the same column twice will throw an exception
	public void throwExceptionForSameIndexCreatedTwice() throws ClassNotFoundException, DBAppException, IOException
	{
		database.createIndex("table", "name", "nameIndex");
		Throwable exception =  assertThrows(DBAppException.class, () -> 
		{database.createIndex("table", "name", "nameIndex");});	
		
		assertEquals("Index already exists",exception.getMessage());

	}
	
	@Test
	// if an index has the same name as one that is already saved, throw exception
	public void throwExceptionForSameIndexName() throws ClassNotFoundException, DBAppException, IOException
	{
		database.createIndex("table", "name", "nameIndex");
		Throwable exception =  assertThrows(DBAppException.class, () -> 
		{database.createIndex("table", "age", "nameIndex");});	
		
		
	}
}
