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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@SuppressWarnings("deprecation")
public class UpdateTests {
	DBApp database;
	Hashtable<String,String> htbl;
	Hashtable<String,Object> colData;
	Table result; // use this to store the result we want (do it by comparing)
	Table banadyMethod; // call the methods on this table
	int pageSize;
	Random random;
	double [] uniqueGPA;
	int [] uniqueAge;
	int [] uniqueID;
	DecimalFormat df;
	
	@BeforeEach
	void init() throws IOException, ClassNotFoundException, DBAppException
	{
		df = new DecimalFormat("#.####");
		df.setRoundingMode(RoundingMode.CEILING);
		pageSize = 3;
		random = new Random();

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
		props.setProperty("MaximumRowsCountinPage", pageSize + "");
		FileWriter writer = new FileWriter(configFile);
		props.store(writer, "");
		writer.close();

		database = new DBApp();

		htbl = new Hashtable<String,String>();
		htbl.put("id", "java.lang.Integer");
		htbl.put("age", "java.lang.Integer");
		htbl.put("name", "java.lang.String");
		htbl.put("gpa", "java.lang.Double");
		
		database.createTable("banadyMethod", "id", htbl);
		database.createTable("result", "id", htbl);
		colData = new Hashtable<String, Object>();

		
		uniqueAge = random.ints(0,500).distinct().limit(pageSize*7).toArray();
		uniqueID = random.ints(0,500).distinct().limit(pageSize*7).toArray();
		uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 1000) / 1000)
                .mapToDouble(Double::doubleValue).limit(pageSize*7).toArray();
		// fill el table be random data
		for(int i = 0;i<pageSize*5;i++)
		{
			int age = uniqueAge[i];
			double gpa = uniqueGPA[i];
			int id = uniqueID[i];
			String name = randomString();
			colData.clear();
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(age));
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("result", colData);
			database.insertIntoTable("banadyMethod", colData);
		}
		
		colData = new Hashtable<String, Object>();
		banadyMethod = database.tables.get(0);
		result = database.tables.get(1);
	}

	private Object deserializeData(String filename) throws ClassNotFoundException, IOException 
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
	
	private boolean comparePage(Page page1, Page page2)
	{
		if(page1.tuples.size()!=page2.tuples.size())
			return false;
		for(int i = 0;i<page1.tuples.size();i++)
			if(!deepCompareTuple(page1.tuples.get(i),page2.tuples.get(i)))
				return false;
			
		return true;
			
	}
	private boolean deepCompareTuple(Tuple tuple1, Tuple tuple2)
	{
		return tuple1.entry.equals(tuple2.entry);
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
	
	// takes the ids in the table, and randomly puts it in the middle
	private int randomID()
	{
		boolean flag = true;
		int res = -1;
		while(true)
		{
			res = uniqueAge[random.nextInt(uniqueAge.length)];
			res+=1;
			for(int i = 0;i<uniqueAge.length;i++)
				if(uniqueAge[i]==res)
					flag = false;
			if(flag)
				break;
		}
		return res;
	}
	//-------------------------------------------------------TESTS--------------------------------------------------------------
	@RepeatedTest(value = 10)
	@Timeout(value = 300)
	@DisplayName("UpdateTable_ForInteger_ShouldUpdateTuple")
	void Updated_Row_Integer() throws ClassNotFoundException, DBAppException, IOException
	{
		int age= 28;
		int id = uniqueID[uniqueID.length-1];
		double gpa = 1.2;
		String name = "Mariam";
		colData.clear();
		colData.put("id", new Integer(id));
		colData.put("age", new Integer(age));
		colData.put("name", new String(name));
		colData.put("gpa", new Double(gpa));
		
		database.insertIntoTable("banadyMethod", colData);
		
		age = 29;
		colData.clear();
		colData.put("id", new Integer(id));
		colData.put("age", new Integer(age));
		colData.put("name", new String(name));
		colData.put("gpa", new Double(gpa));
		database.insertIntoTable("result", colData);
		
		colData.clear();
		colData.put("age", new Integer(age));
		
		database.updateTable("banadyMethod", id + "", colData);
		
		assertTrue(banadyMethod.pageNames.size()==result.pageNames.size(),"Expected the number of pages to be " 
				+ banadyMethod.pageNames.size()	+ ", but instead got " +  result.pageNames.size());
		for(int i = 0;i<result.pageNames.size();i++)
		{
			Page banadyMethodPage = (Page) deserializeData(banadyMethod.filepath + banadyMethod.name + i);
			Page resultPage = (Page) deserializeData(result.filepath + result.name + i);
			assertTrue(comparePage(banadyMethodPage,resultPage), "\n Expected the tuples in page " + banadyMethodPage.name + " to be: \n"
					+ resultPage + "\n but instead got: \n" + banadyMethodPage);
		}
		
	}
	
	// Check that double was updated after calling method
	@RepeatedTest(value = 10)
	@Timeout(value = 300)
	@DisplayName("UpdateTable_ForDouble_ShouldUpdateTuple")
	void Updated_Row_Double() throws ClassNotFoundException, DBAppException, IOException
	{
		int age= 28;
		int id = uniqueID[uniqueID.length-1];
		double gpa = 1.2;
		String name = "Mariam";
		colData.clear();
		colData.put("id", new Integer(id));
		colData.put("age", new Integer(age));
		colData.put("name", new String(name));
		colData.put("gpa", new Double(gpa));
		
		database.insertIntoTable("banadyMethod", colData);
		
		gpa = 0.9;
		colData.clear();
		colData.put("id", new Integer(id));
		colData.put("age", new Integer(age));
		colData.put("name", new String(name));
		colData.put("gpa", new Double(gpa));
		database.insertIntoTable("result", colData);
		
		colData.clear();
		colData.put("gpa", new Double(gpa));
		
		database.updateTable("banadyMethod", id + "", colData);
		
		assertTrue(banadyMethod.pageNames.size()==result.pageNames.size(),"Expected the number of pages to be " 
				+ banadyMethod.pageNames.size()	+ ", but instead got " +  result.pageNames.size());
		for(int i = 0;i<result.pageNames.size();i++)
		{
			Page banadyMethodPage = (Page) deserializeData(banadyMethod.filepath + banadyMethod.name + i);
			Page resultPage = (Page) deserializeData(result.filepath + result.name + i);
			assertTrue(comparePage(banadyMethodPage,resultPage), "\n Expected the tuples in page " + banadyMethodPage.name + " to be: \n"
					+ resultPage + "\n but instead got: \n" + banadyMethodPage);
		}
		
	}
	
	// Check that String was updated after calling method
	@RepeatedTest(value = 10)
	@Timeout(value = 300)
	@DisplayName("UpdateTable_ForString_ShouldUpdateTuple")
	void Updated_Row_String() throws ClassNotFoundException, DBAppException, IOException
	{
		int age= 28;
		int id = uniqueID[uniqueID.length-1];
		double gpa = 1.2;
		String name = "Mariam";
		colData.clear();
		colData.put("id", new Integer(id));
		colData.put("age", new Integer(age));
		colData.put("name", new String(name));
		colData.put("gpa", new Double(gpa));
		
		database.insertIntoTable("banadyMethod", colData);
		
		name = "Marioma";
		colData.clear();
		colData.put("id", new Integer(id));
		colData.put("age", new Integer(age));
		colData.put("name", new String(name));
		colData.put("gpa", new Double(gpa));
		database.insertIntoTable("result", colData);
		
		colData.clear();
		colData.put("name", new String(name));
		
		database.updateTable("banadyMethod", id + "", colData);
		
		assertTrue(banadyMethod.pageNames.size()==result.pageNames.size(),"Expected the number of pages to be " 
				+ banadyMethod.pageNames.size()	+ ", but instead got " +  result.pageNames.size());
		for(int i = 0;i<result.pageNames.size();i++)
		{
			Page banadyMethodPage = (Page) deserializeData(banadyMethod.filepath + banadyMethod.name + i);
			Page resultPage = (Page) deserializeData(result.filepath + result.name + i);
			assertTrue(comparePage(banadyMethodPage,resultPage), "\n Expected the tuples in page " + banadyMethodPage.name + " to be: \n"
					+ resultPage + "\n but instead got: \n" + banadyMethodPage);
		}
	}
	
	// if the row we are updating does not have the same datatype in the htbl, then an exception should be thrown
	@Test
	@DisplayName("UpdateTable_WithWrongDataTypeForDoubleColumn_ShouldThrowException")
	void ThrowExceptionForWrongDataTypeDouble() throws ClassNotFoundException, DBAppException, IOException
	{
		int id = uniqueID[uniqueID.length-1];
		int age= 28;
		double gpa = 1.2;
		String name = "Mariam";
		colData.clear();
		colData.put("id", new Integer(id));
		colData.put("age", new Integer(age));
		colData.put("name", new String(name));
		colData.put("gpa", new Double(gpa));
		
		database.insertIntoTable("banadyMethod", colData);
		
		colData.clear();
		colData.put("gpa", new String(name));
		
		assertThrows(DBAppException.class, () -> 
		{database.updateTable("banadyMethod", id + "", colData);});	
	}
	
	// if the row we are updating does not have the same datatype in the htbl, then an exception should be thrown
	@Test
	@DisplayName("UpdateTable_WithWrongDataTypeForIntegerColumn_ShouldThrowException")
	void ThrowExceptionForWrongDataTypeInteger() throws ClassNotFoundException, DBAppException, IOException
	{
		int id = uniqueID[uniqueID.length-1];
		int age= 28;
		double gpa = 1.2;
		String name = "Mariam";
		colData.clear();
		colData.put("id", new Integer(id));
		colData.put("age", new Integer(age));
		colData.put("name", new String(name));
		colData.put("gpa", new Double(gpa));
		
		database.insertIntoTable("banadyMethod", colData);
		
		colData.clear();
		colData.put("age", new String(name));
		
		assertThrows(DBAppException.class, () -> 
		{database.updateTable("banadyMethod", id + "", colData);});	
	}
	
	// if the row we are updating does not have the same datatype in the htbl, then an exception should be thrown
	@Test
	@DisplayName("UpdateTable_WithWrongDataTypeForStringColumn_ShouldThrowException")
	void ThrowExceptionForWrongDataTypeString() throws ClassNotFoundException, DBAppException, IOException
	{
		int id = uniqueID[uniqueID.length-1];
		int age= 28;
		double gpa = 1.2;
		String name = "Mariam";
		colData.clear();
		colData.put("id", new Integer(id));
		colData.put("age", new Integer(age));
		colData.put("name", new String(name));
		colData.put("gpa", new Double(gpa));
		
		database.insertIntoTable("banadyMethod", colData);
		
		colData.clear();
		colData.put("name", new Integer(69));
		
		assertThrows(DBAppException.class, () -> 
		{database.updateTable("banadyMethod", id + "", colData);});	
	}
	
	// if a column name does not exist, an exception should be thrown
	@Test
	@DisplayName("UpdateTable_ForNonExistentColumn_ShouldThrowException")
	void ThrowExceptionForWrongColumnName() throws ClassNotFoundException, DBAppException, IOException
	{
		int id = uniqueID[uniqueID.length-1];
		int age= 28;
		double gpa = 1.2;
		String name = "Mariam";
		colData.clear();
		colData.put("id", new Integer(id));
		colData.put("age", new Integer(age));
		colData.put("name", new String(name));
		colData.put("gpa", new Double(gpa));
		
		database.insertIntoTable("banadyMethod", colData);
		
		colData.clear();
		colData.put("namiswan", new String(name));
		
		assertThrows(DBAppException.class, () -> 
		{database.updateTable("banadyMethod", id + "", colData);});	
	}
	
	// Check that index was updated if one exists
	@RepeatedTest(value = 5)
	@DisplayName("UpdateTable_WithIndexedString_ShouldUpdateIndex")
	void Index_Updated_String() throws ClassNotFoundException, DBAppException, IOException
	{
		database.createIndex("result", "name", "nameIndex");
		database.createIndex("banadyMethod", "gpa", "gpaIndex");
		database.createIndex("banadyMethod", "name", "nameIndex");
		database.createIndex("banadyMethod", "age", "ageIndex");
		
		int age= 28;
		int id = uniqueID[uniqueID.length-1];
		double gpa = 1.2;
		String name = "Mariam";
		colData.clear();
		colData.put("id", new Integer(id));
		colData.put("age", new Integer(age));
		colData.put("name", new String(name));
		colData.put("gpa", new Double(gpa));
		
		database.insertIntoTable("banadyMethod", colData);
		
		name = "Marioma";
		colData.clear();
		colData.put("id", new Integer(id));
		colData.put("age", new Integer(age));
		colData.put("name", new String(name));
		colData.put("gpa", new Double(gpa));
		database.insertIntoTable("result", colData);
		
		colData.clear();
		colData.put("name", new String(name));
		
		database.updateTable("banadyMethod", id + "", colData);
		
		Index resultIndex = (Index) deserializeData(result.filepath+"/indices/" + "nameIndex.ser");
		Index banadyMethodIndex = (Index) deserializeData(banadyMethod.filepath+"/indices/" + "nameIndex.ser");
		ArrayList<Vector<String>> resultPointers = resultIndex.searchGreaterThan(new Datatype(""), true);
		ArrayList<Vector<String>> banadyMethodPointers = banadyMethodIndex.searchGreaterThan(new Datatype(""), true);
		assertEquals(resultPointers.size(), banadyMethodPointers.size());
		ArrayList<String> resultPointerNumbers = new ArrayList<String>();
		ArrayList<String> banadyMethodPointerNumbers = new ArrayList<String>();
		for(int i = 0;i<resultPointers.size();i++)
		{
			String page = resultPointers.get(i).get(0);
			resultPointerNumbers.add(page.substring(page.length(),page.length()));
			
			page = banadyMethodPointers.get(i).get(0);
			banadyMethodPointerNumbers.add(page.substring(page.length(),page.length()));
		}
		assertTrue(banadyMethodPointerNumbers.equals(resultPointerNumbers), 
				"Expected " + resultPointers + ", but instead got " + banadyMethodPointers);
	}
	
	// Check that index was updated if one exists
	@RepeatedTest(value = 5)
	@DisplayName("UpdateTable_WithIndexedInteger_ShouldUpdateIndex")
	void Index_Updated_Integer() throws ClassNotFoundException, DBAppException, IOException
	{
		database.createIndex("result", "age", "ageIndex");
		database.createIndex("banadyMethod", "gpa", "gpaIndex");
		database.createIndex("banadyMethod", "name", "nameIndex");
		database.createIndex("banadyMethod", "age", "ageIndex");
		
		int age= 28;
		int id = uniqueID[uniqueID.length-1];
		double gpa = 1.2;
		String name = "Mariam";
		colData.clear();
		colData.put("id", new Integer(id));
		colData.put("age", new Integer(age));
		colData.put("name", new String(name));
		colData.put("gpa", new Double(gpa));
		
		database.insertIntoTable("banadyMethod", colData);
		
		age = 30;
		colData.clear();
		colData.put("id", new Integer(id));
		colData.put("age", new Integer(age));
		colData.put("name", new String(name));
		colData.put("gpa", new Double(gpa));
		database.insertIntoTable("result", colData);
		
		colData.clear();
		colData.put("age", new Integer(age));
		
		database.updateTable("banadyMethod", id + "", colData);
		
		Index resultIndex = (Index) deserializeData(result.filepath+"/indices/" + "ageIndex.ser");
		Index banadyMethodIndex = (Index) deserializeData(banadyMethod.filepath+"/indices/" + "ageIndex.ser");
		ArrayList<Vector<String>> resultPointers = resultIndex.searchGreaterThan(new Datatype(0), true);
		ArrayList<Vector<String>> banadyMethodPointers = banadyMethodIndex.searchGreaterThan(new Datatype(0), true);
		assertEquals(resultPointers.size(), banadyMethodPointers.size());
		ArrayList<String> resultPointerNumbers = new ArrayList<String>();
		ArrayList<String> banadyMethodPointerNumbers = new ArrayList<String>();
		for(int i = 0;i<resultPointers.size();i++)
		{
			String page = resultPointers.get(i).get(0);
			resultPointerNumbers.add(page.substring(page.length(),page.length()));
			
			page = banadyMethodPointers.get(i).get(0);
			banadyMethodPointerNumbers.add(page.substring(page.length(),page.length()));
		}
		assertTrue(banadyMethodPointerNumbers.equals(resultPointerNumbers), 
				"Expected " + resultPointers + ", but instead got " + banadyMethodPointers);
	}
	
	// Check that index was updated if one exists
	@RepeatedTest(value = 5)
	@DisplayName("UpdateTable_WithIndexedDouble_ShouldUpdateIndex")
	void Index_Updated_Double() throws ClassNotFoundException, DBAppException, IOException
	{
		database.createIndex("result", "gpa", "gpaIndex");
		database.createIndex("banadyMethod", "gpa", "gpaIndex");
		database.createIndex("banadyMethod", "name", "nameIndex");
		database.createIndex("banadyMethod", "age", "ageIndex");
		
		int age= 28;
		int id = uniqueID[uniqueID.length-1];
		double gpa = 1.2;
		String name = "Mariam";
		colData.clear();
		colData.put("id", new Integer(id));
		colData.put("age", new Integer(age));
		colData.put("name", new String(name));
		colData.put("gpa", new Double(gpa));
		
		database.insertIntoTable("banadyMethod", colData);
		
		gpa = 0.7;
		colData.clear();
		colData.put("id", new Integer(id));
		colData.put("age", new Integer(age));
		colData.put("name", new String(name));
		colData.put("gpa", new Double(gpa));
		database.insertIntoTable("result", colData);
		
		colData.clear();
		colData.put("gpa", new Double(gpa));
		
		database.updateTable("banadyMethod", id + "", colData);
		
		Index resultIndex = (Index) deserializeData(result.filepath+"/indices/" + "gpaIndex.ser");
		Index banadyMethodIndex = (Index) deserializeData(banadyMethod.filepath+"/indices/" + "gpaIndex.ser");
		ArrayList<Vector<String>> resultPointers = resultIndex.searchGreaterThan(new Datatype(0.0), true);
		ArrayList<Vector<String>> banadyMethodPointers = banadyMethodIndex.searchGreaterThan(new Datatype(0.0), true);
		assertEquals(resultPointers.size(), banadyMethodPointers.size());
		ArrayList<String> resultPointerNumbers = new ArrayList<String>();
		ArrayList<String> banadyMethodPointerNumbers = new ArrayList<String>();
		for(int i = 0;i<resultPointers.size();i++)
		{
			String page = resultPointers.get(i).get(0);
			resultPointerNumbers.add(page.substring(page.length(),page.length()));
			
			page = banadyMethodPointers.get(i).get(0);
			banadyMethodPointerNumbers.add(page.substring(page.length(),page.length()));
		}
		assertTrue(banadyMethodPointerNumbers.equals(resultPointerNumbers), 
				"Expected " + resultPointers + ", but instead got " + banadyMethodPointers);
	}
	
	// if the primary key updated does not exist; do not update (or throw exception?)
	@Test
	@DisplayName("UpdateTable_WithClusteringKeyNotInTable_ShouldNotUpdate")
	void No_Update() throws ClassNotFoundException, IOException, DBAppException
	{
		colData.clear();
		colData.put("name", new String("Sherif"));
		
		database.updateTable("banadyMethod", 3020 + "", colData);
		
		assertTrue(banadyMethod.pageNames.size()==result.pageNames.size(),"Expected the number of pages to be " 
				+ banadyMethod.pageNames.size()	+ ", but instead got " +  result.pageNames.size());
		for(int i = 0;i<result.pageNames.size();i++)
		{
			Page banadyMethodPage = (Page) deserializeData(banadyMethod.filepath + banadyMethod.name + i);
			Page resultPage = (Page) deserializeData(result.filepath + result.name + i);
			assertTrue(comparePage(banadyMethodPage,resultPage), "\n Expected the tuples in page " + banadyMethodPage.name + " to be: \n"
					+ resultPage + "\n but instead got: \n" + banadyMethodPage);
		}
	}
}
