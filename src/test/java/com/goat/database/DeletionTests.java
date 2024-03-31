package com.goat.database;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
public class DeletionTests {
	// TODO test exceptions (5odo copy paste mein insertion tests)
	DBApp database;
	Hashtable<String,String> htbl;
	Hashtable<String,Object> colData;
	Table result; // use this to store the result we want (do it by comparing)
	Table banadyMethod; // call the methods on this table
	int pageSize;
	Random random;

	@BeforeEach
	void init() throws IOException, ClassNotFoundException, DBAppException
	{
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
	
	//-------------------------------------------------------TESTS--------------------------------------------------------------
	//This checks if one tuple is deleted from the page
	@Test
	void Delete_Tuple_From_Page() throws IOException, DBAppException, ClassNotFoundException
	{
		colData.put("id", new Integer(1));
		colData.put("age", new Integer(5));
		colData.put("name", new String("3oraby"));
		colData.put("gpa", new Double(5.2));
		database.insertIntoTable("result", colData);
		database.insertIntoTable("banadyMethod", colData);
		
		colData.clear();
		colData.put("id", new Integer(5));
		colData.put("age", new Integer(5));
		colData.put("name", new String("3oraby"));
		colData.put("gpa", new Double(5.2));
		database.insertIntoTable("banadyMethod", colData);
		
		colData.clear();
		colData.put("id", new Integer(5));
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
	void No_Tuples_Deleted_For_False_Info() throws ClassNotFoundException, DBAppException, IOException
	{
		colData.put("id", new Integer(1));
		colData.put("age", new Integer(5));
		colData.put("name", new String("3oraby"));
		colData.put("gpa", new Double(5.2));
		database.insertIntoTable("result", colData);
		database.insertIntoTable("banadyMethod", colData);
		
		colData.clear();
		colData.put("id", new Integer(5));
		colData.put("age", new Integer(5));
		colData.put("name", new String("3oraby"));
		colData.put("gpa", new Double(5.2));
		database.insertIntoTable("result", colData);
		database.insertIntoTable("banadyMethod", colData);

		colData.clear();
		colData.put("id", new Integer(20));
		colData.put("age", new Integer(2093));
		colData.put("name", new String("mesh 3oraby"));
		colData.put("gpa", new Double(5.23));
		database.deleteFromTable("banadyMethod", colData);
		
		Page banadyMethodPage = (Page) deserializeData(banadyMethod.filepath + banadyMethod.name + 0);
		Page resultPage = (Page) deserializeData(result.filepath + result.name + 0);

		assertTrue(banadyMethodPage.tuples.size()==2, "Expected two tuples to be in the page, but instead there are "
				+ banadyMethodPage.tuples.size() + " tuples in the page");
		assertTrue(comparePage(banadyMethodPage,resultPage), "\nExpected the tuples to be: \n" + resultPage + "\n but instead"
				+ " got: \n" + banadyMethodPage);
		
	}

	//This checks that no gaps are left when deleting from a page
	@Test
	@Disabled
	void No_Empty_Space_In_Page_After_Deletion() throws DBAppException, ClassNotFoundException, IOException
	{


	}

	// This checks that if we delete the last tuple from a page, it
	// also deletes the entire page
	@Test
	@Disabled
	void Delete_Page_Once_Last_Tuple_Is_Deleted() throws DBAppException, IOException, ClassNotFoundException
	{

	}

	// This checks that if multiple tuples satisfy the deletion condition for an int,
	// all of them are also deleted
	@Test
	void Delete_Multiple_Tuples_From_Page_Integer() throws ClassNotFoundException, DBAppException, IOException
	{
		int deletionAge = random.nextInt();
		int [] uniqueAge = random.ints(0,500).distinct().limit(pageSize*7).toArray();
		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 1000) / 1000)
                .mapToDouble(Double::doubleValue).limit(pageSize*7).toArray();
		// we do the following in the loop: create one tuple completely random and put it in both tables
		// and create another tuple that has an age that we will use to delete when we call the method
		for(int i = 0;i<pageSize*5;i++)
		{
			int age =  uniqueAge[i];
			double gpa = uniqueGPA[i];
			String name = randomString();
			colData.clear();
			colData.put("id", new Integer(i*age));
			colData.put("age", new Integer(age));
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("result", colData);
			database.insertIntoTable("banadyMethod", colData);
			
			colData.clear();
			colData.put("id", new Integer(i+deletionAge)); // to get a unqiue id we add deletion age to it
			colData.put("age", new Integer(deletionAge));
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
		}
		colData.clear();
		colData.put("age", new Integer(deletionAge));
		database.deleteFromTable("banadyMethod", colData);
		
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
	
	// This checks that if multiple tuples satisfy the deletion condition for an double,
	// all of them are also deleted
	@Test
	void Delete_Multiple_Tuples_From_Page_Double() throws ClassNotFoundException, DBAppException, IOException
	{
		int [] uniqueAge = random.ints(0,500).distinct().limit(pageSize*7).toArray();
		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 1000) / 1000)
                .mapToDouble(Double::doubleValue).limit(pageSize*7).toArray();
		double deletionGPA = uniqueGPA[uniqueGPA.length-1];
		// we do the following in the loop: create one tuple completely random and put it in both tables
		// and create another tuple that has gpa that we will use to delete when we call the method
		for(int i = 0;i<pageSize*5;i++)
		{
			int age = uniqueAge[i];
			double gpa = uniqueGPA[i];
			String name = randomString();
			colData.clear();
			colData.put("id", new Integer(i*age));
			colData.put("age", new Integer(age));
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("result", colData);
			database.insertIntoTable("banadyMethod", colData);
			
			colData.clear();
			colData.put("id", new Integer(age/2)); 
			colData.put("age", new Integer(age));
			colData.put("name", new String(name));
			colData.put("gpa", new Double(deletionGPA));
			database.insertIntoTable("banadyMethod", colData);
		}
		colData.clear();
		colData.put("age", new Double(deletionGPA));
		database.deleteFromTable("banadyMethod", colData);
		
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

	// This checks that if multiple tuples satisfy the deletion condition for an string,
	// all of them are also deleted
	@Test
	void Delete_Multiple_Tuples_From_Page_String() throws ClassNotFoundException, DBAppException, IOException
	{
		String deletionName = randomString();
		int [] uniqueAge = random.ints(0,500).distinct().limit(pageSize*7).toArray();
		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 1000) / 1000)
                .mapToDouble(Double::doubleValue).limit(pageSize*7).toArray();
		// we do the following in the loop: create one tuple completely random and put it in both tables
		// and create another tuple that has a name that we will use to delete when we call the method
		for(int i = 0;i<pageSize*5;i++)
		{
			int age =  uniqueAge[i];
			double gpa = uniqueGPA[i];
			String name = randomString();
			colData.clear();
			colData.put("id", new Integer(i*age));
			colData.put("age", new Integer(age));
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("result", colData);
			database.insertIntoTable("banadyMethod", colData);
			
			colData.clear();
			colData.put("id", new Integer(age/2));
			colData.put("age", new Integer(age));
			colData.put("name", new String(deletionName));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
		}
		colData.clear();
		colData.put("age", new String(deletionName));
		database.deleteFromTable("banadyMethod", colData);
		
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
	
	// This checks that if I have a condition with integer and string,
	// all of tuples meeting that condition are deleted
	@Test
	@Disabled
	void Delete_Multiple_Tuple_With_Multiple_Deletion_Conditions_Integer_And_String()
	{
		
	}
	
	@Test
	@Disabled
	void Delete_Multiple_Tuple_With_Multiple_Deletion_Conditions_Integer_And_Double()
	{
		
	}
	
	@Test
	@Disabled
	void Delete_Multiple_Tuple_With_Multiple_Deletion_Conditions_Double_And_String()
	{
		
	}
	
	@Test
	@Disabled
	void Delete_Multiple_Tuple_With_Multiple_Deletion_Conditions_Integer_And_Double_And_String()
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
