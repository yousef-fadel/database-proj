package com.goat.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
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
	int [] uniqueID;
	
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
	
	private int insertRandomTuples(int numberOfPages) throws ClassNotFoundException, DBAppException, IOException
	{
		uniqueID = random.ints(0,5000).distinct().limit(pageSize*numberOfPages*10).toArray();
		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 1000) / 1000)
                .mapToDouble(Double::doubleValue).limit(pageSize*2*numberOfPages).toArray();
		
		for(int i = 0 ;i<pageSize*numberOfPages;i++)
		{
			int id =  uniqueID[i];
			double gpa = uniqueGPA[i];
			int age = uniqueID[i];
			String name = randomString();
			colData.clear();
			colData.put("id", new Integer(id));
			colData.put("age", age);
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
			database.insertIntoTable("result", colData);
			
		}
		return pageSize*numberOfPages;
	}
	
	//-------------------------------------------------------TESTS--------------------------------------------------------------
	//This checks if one tuple is deleted from the page
	@Test
	@DisplayName("DeleteFromTable_ForASingleTuple_ShouldRemoveTupleFromPage")
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
	@DisplayName("DeleteFromTable_WithDataNotInTable_ShouldLeaveTableIntact")
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



	// This checks that if we delete the last tuple from a page, it
	// also deletes the entire page
	@Test
	@DisplayName("DeleteFromTable_AllTuplesInPage_ShouldDeletePageToo")
	void Delete_Page_Once_Last_Tuple_Is_Deleted() throws DBAppException, IOException, ClassNotFoundException
	{
		int pages = 8;
		insertRandomTuples(pages);
		
		int intRandomPageNo = random.nextInt(pages);
		Page randomPage = (Page) deserializeData(banadyMethod.filepath + banadyMethod.name + intRandomPageNo);
		// loop might break; prolly not tho as randomPage should not be deleted from?
		for(Tuple currTuple:randomPage.tuples)
		{
			colData.clear();
			colData.put("id", new Integer((int) currTuple.entry.get("id")));
			database.deleteFromTable("banadyMethod", colData);
		}
		
		assertTrue(!banadyMethod.pageNames.contains(randomPage.name),"Found page name " + randomPage.name + " in the array"
				+ " containing page names; it should be deleted");
		
		File deletedPageFilePath = new File(randomPage.pageFilepath);
		assertTrue(!deletedPageFilePath.exists(), "Expected to find the page deleted, but still found it in " + randomPage.pageFilepath);
	}

	// This checks that if multiple tuples satisfy the deletion condition for an int,
	// all of them are also deleted
	@Test
	@DisplayName("DeleteFromTable_WithIntegerData_ShouldRemoveAllTuplesSatisfyingIntegerData")
	void Delete_Multiple_Tuples_From_Page_Integer() throws ClassNotFoundException, DBAppException, IOException
	{
		int deletionAge = random.nextInt();
		int [] uniqueID = random.ints(500,1000).distinct().limit(pageSize*7).toArray();
		int [] uniqueAge = random.ints(0,500).distinct().limit(pageSize*7).toArray();
		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 1000) / 1000)
                .mapToDouble(Double::doubleValue).limit(pageSize*7).toArray();
		// we do the following in the loop: create one tuple completely random and put it in both tables
		// and create another tuple that has an age that we will use to delete when we call the method
		for(int i = 0;i<pageSize*5;i++)
		{
			int id = uniqueID[i];
			int age =  uniqueAge[i];
			double gpa = uniqueGPA[i];
			String name = randomString();
			colData.clear();
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(age));
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("result", colData);
			database.insertIntoTable("banadyMethod", colData);
			
			colData.clear();
			colData.put("id", new Integer(age));
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
			Page banadyMethodPage = (Page) deserializeData(banadyMethod.filepath + banadyMethod.pageNames.get(i));
			Page resultPage = (Page) deserializeData(result.filepath + result.pageNames.get(i));
			assertTrue(comparePage(banadyMethodPage,resultPage), "\n Expected the tuples in page " + banadyMethodPage.name + " to be: \n"
					+ resultPage + "\n but instead got: \n" + banadyMethodPage);
		}
		
		
	}
	
	// This checks that if multiple tuples satisfy the deletion condition for an double,
	// all of them are also deleted
	@Test
	@DisplayName("DeleteFromTable_WithDoubleData_ShouldRemoveAllTuplesSatisfyingDoubleData")
	void Delete_Multiple_Tuples_From_Page_Double() throws ClassNotFoundException, DBAppException, IOException
	{
		int [] uniqueAge = random.ints(0,500).distinct().limit(pageSize*7).toArray();
		int [] uniqueID = random.ints(500,1000).distinct().limit(pageSize*7).toArray();
		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 1000) / 1000)
                .mapToDouble(Double::doubleValue).limit(pageSize*7).toArray();
		double deletionGPA = uniqueGPA[uniqueGPA.length-1];
		// we do the following in the loop: create one tuple completely random and put it in both tables
		// and create another tuple that has gpa that we will use to delete when we call the method
		for(int i = 0;i<pageSize*5;i++)
		{
			int age = uniqueAge[i];
			int id = uniqueID[i];
			double gpa = uniqueGPA[i];
			String name = randomString();
			colData.clear();
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(age));
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("result", colData);
			database.insertIntoTable("banadyMethod", colData);
			
			colData.clear();
			colData.put("id", new Integer(age)); 
			colData.put("age", new Integer(age));
			colData.put("name", new String(name));
			colData.put("gpa", new Double(deletionGPA));
			database.insertIntoTable("banadyMethod", colData);
		}
		colData.clear();
		colData.put("gpa", new Double(deletionGPA));
		database.deleteFromTable("banadyMethod", colData);
		
		assertTrue(banadyMethod.pageNames.size()==result.pageNames.size(),"Expected the number of pages to be " 
				+ banadyMethod.pageNames.size()	+ ", but instead got " +  result.pageNames.size());
		for(int i = 0;i<result.pageNames.size();i++)
		{
			Page banadyMethodPage = (Page) deserializeData(banadyMethod.filepath + banadyMethod.pageNames.get(i));
			Page resultPage = (Page) deserializeData(result.filepath + result.pageNames.get(i));
			assertTrue(comparePage(banadyMethodPage,resultPage), "\n Expected the tuples in page " + banadyMethodPage.name + " to be: \n"
					+ resultPage + "\n but instead got: \n" + banadyMethodPage);
		}
		
		
	}

	// This checks that if multiple tuples satisfy the deletion condition for an string,
	// all of them are also deleted
	@Test
	@DisplayName("DeleteFromTable_WithStringData_ShouldRemoveAllTuplesSatisfyingStringData")
	void Delete_Multiple_Tuples_From_Page_String() throws ClassNotFoundException, DBAppException, IOException
	{
		String deletionName = randomString();
		int [] uniqueAge = random.ints(0,500).distinct().limit(pageSize*7).toArray();
		int [] uniqueID = random.ints(500,1000).distinct().limit(pageSize*7).toArray();
		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 1000) / 1000)
                .mapToDouble(Double::doubleValue).limit(pageSize*7).toArray();
		// we do the following in the loop: create one tuple completely random and put it in both tables
		// and create another tuple that has a name that we will use to delete when we call the method
		for(int i = 0;i<pageSize*5;i++)
		{
			int age =  uniqueAge[i];
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
			
			colData.clear();
			colData.put("id", new Integer(age));
			colData.put("age", new Integer(age));
			colData.put("name", new String(deletionName));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
		}
		colData.clear();
		colData.put("name", new String(deletionName));
		database.deleteFromTable("banadyMethod", colData);
		
		assertTrue(banadyMethod.pageNames.size()==result.pageNames.size(),"Expected the number of pages to be " 
				    + banadyMethod.pageNames.size()	+ ", but instead got " +  result.pageNames.size());
		for(int i = 0;i<result.pageNames.size();i++)
		{
			Page banadyMethodPage = (Page) deserializeData(banadyMethod.filepath + banadyMethod.pageNames.get(i));
			Page resultPage = (Page) deserializeData(result.filepath + result.pageNames.get(i));
			assertTrue(comparePage(banadyMethodPage,resultPage), "\n Expected the tuples in page " + banadyMethodPage.name + " to be: \n"
					+ resultPage + "\n but instead got: \n" + banadyMethodPage);
		}
		
		
	}
	
	// This checks that if I have a condition with integer and string,
	// all of tuples meeting that condition are deleted
	@Test
	@DisplayName("DeleteFromTable_WithIntegerAndStringData_ShouldRemoveAllTuplesSatisfyingIntegerAndStringData")
	void Delete_Multiple_Tuple_With_Multiple_Deletion_Conditions_Integer_And_String() throws ClassNotFoundException, DBAppException, IOException
	{

		int [] uniqueAge = random.ints(0,500).distinct().limit(pageSize*7).toArray();
		int [] uniqueID = random.ints(500,1000).distinct().limit(pageSize*7).toArray();
		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 1000) / 1000)
                .mapToDouble(Double::doubleValue).limit(pageSize*7).toArray();
		String deletionName = "Machi wa";
		int deletionAge = 696969;
		// we do the following in the loop: create one tuple completely random and put it in both tables
		// and create another tuple that has a name that we will use to delete when we call the method
		for(int i = 0;i<pageSize*5;i++)
		{
			int age =  uniqueAge[i];
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
			
			colData.clear();
			colData.put("id", new Integer(age));
			colData.put("age", new Integer(deletionAge));
			colData.put("name", new String(deletionName));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
			
		}
		colData.clear();
		colData.put("age", new Integer(deletionAge));
		colData.put("name", new String(deletionName));
		database.deleteFromTable("banadyMethod", colData);
		
		assertTrue(banadyMethod.pageNames.size()==result.pageNames.size(),"Expected the number of pages to be " 
				    + banadyMethod.pageNames.size()	+ ", but instead got " +  result.pageNames.size());
		for(int i = 0;i<result.pageNames.size();i++)
		{
			Page banadyMethodPage = (Page) deserializeData(banadyMethod.filepath + banadyMethod.pageNames.get(i));
			Page resultPage = (Page) deserializeData(result.filepath + result.pageNames.get(i));
			assertTrue(comparePage(banadyMethodPage,resultPage), "\n Expected the tuples in page " + banadyMethodPage.name + " to be: \n"
					+ resultPage + "\n but instead got: \n" + banadyMethodPage);
		}
		
		
	}
	
	@Test
	@DisplayName("DeleteFromTable_WithIntegerAndDoubleData_ShouldRemoveAllTuplesSatisfyingIntegerAndDoubleData")
	void Delete_Multiple_Tuple_With_Multiple_Deletion_Conditions_Integer_And_Double() throws ClassNotFoundException, DBAppException, IOException
	{

		int [] uniqueAge = random.ints(0,500).distinct().limit(pageSize*7).toArray();
		int [] uniqueID = random.ints(500,1000).distinct().limit(pageSize*7).toArray();
		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 1000) / 1000)
                .mapToDouble(Double::doubleValue).limit(pageSize*7).toArray();
		double deletionGPA = 7.0;
		int deletionAge = 6029;
		// we do the following in the loop: create one tuple completely random and put it in both tables
		// and create another tuple that has a name that we will use to delete when we call the method
		for(int i = 0;i<pageSize*5;i++)
		{
			int age =  uniqueAge[i];
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
			
			colData.clear();
			colData.put("id", new Integer(age));
			colData.put("age", new Integer(deletionAge));
			colData.put("name", new String(name));
			colData.put("gpa", new Double(deletionGPA));
			database.insertIntoTable("banadyMethod", colData);

		}
		colData.clear();
		colData.put("age", new Integer(deletionAge));
		colData.put("gpa", new Double(deletionGPA));
		database.deleteFromTable("banadyMethod", colData);
		
		assertTrue(banadyMethod.pageNames.size()==result.pageNames.size(),"Expected the number of pages to be " 
				    + banadyMethod.pageNames.size()	+ ", but instead got " +  result.pageNames.size());
		for(int i = 0;i<result.pageNames.size();i++)
		{
			Page banadyMethodPage = (Page) deserializeData(banadyMethod.filepath + banadyMethod.pageNames.get(i));
			Page resultPage = (Page) deserializeData(result.filepath + result.pageNames.get(i));
			assertTrue(comparePage(banadyMethodPage,resultPage), "\n Expected the tuples in page " + banadyMethodPage.name + " to be: \n"
					+ resultPage + "\n but instead got: \n" + banadyMethodPage);
		}
		
		
	}
	
	@Test
	@DisplayName("DeleteFromTable_WithStringAndDoubleData_ShouldRemoveAllTuplesSatisfyingStringAndDoubleData")
	void Delete_Multiple_Tuple_With_Multiple_Deletion_Conditions_Double_And_String() throws ClassNotFoundException, DBAppException, IOException
	{
		int [] uniqueAge = random.ints(0,500).distinct().limit(pageSize*7).toArray();
		int [] uniqueID = random.ints(500,1000).distinct().limit(pageSize*7).toArray();
		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 1000) / 1000)
                .mapToDouble(Double::doubleValue).limit(pageSize*7).toArray();
		String deletionName = "Farida";
		double deletionGPA = 80.2;
		// we do the following in the loop: create one tuple completely random and put it in both tables
		// and create another tuple that has a name that we will use to delete when we call the method
		for(int i = 0;i<pageSize*5;i++)
		{
			int age =  uniqueAge[i];
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
			
			colData.clear();
			colData.put("id", new Integer(age));
			colData.put("age", new Integer(age));
			colData.put("name", new String(deletionName));
			colData.put("gpa", new Double(deletionGPA));
			database.insertIntoTable("banadyMethod", colData);
			
		}
		colData.clear();
		colData.put("gpa", new Double(deletionGPA));
		colData.put("name", new String(deletionName));
		database.deleteFromTable("banadyMethod", colData);
		
		assertTrue(banadyMethod.pageNames.size()==result.pageNames.size(),"Expected the number of pages to be " 
				    + banadyMethod.pageNames.size()	+ ", but instead got " +  result.pageNames.size());
		for(int i = 0;i<result.pageNames.size();i++)
		{
			Page banadyMethodPage = (Page) deserializeData(banadyMethod.filepath + banadyMethod.pageNames.get(i));
			Page resultPage = (Page) deserializeData(result.filepath + result.pageNames.get(i));
			assertTrue(comparePage(banadyMethodPage,resultPage), "\n Expected the tuples in page " + banadyMethodPage.name + " to be: \n"
					+ resultPage + "\n but instead got: \n" + banadyMethodPage);
		}
		
		
	}
	
	@Test
	@DisplayName("DeleteFromTable_WithIntegerAndDoubleAndStringData_ShouldRemoveAllTuplesSatisfyingIntegerAndDoubleAndStringData")
	void Delete_Multiple_Tuple_With_Multiple_Deletion_Conditions_Integer_And_Double_And_String() throws ClassNotFoundException, DBAppException, IOException
	{

		int [] uniqueAge = random.ints(0,500).distinct().limit(pageSize*7).toArray();
		int [] uniqueID = random.ints(500,1000).distinct().limit(pageSize*7).toArray();
		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 1000) / 1000)
                .mapToDouble(Double::doubleValue).limit(pageSize*7).toArray();
		String deletionName = "Mohamed";
		double deletionGPA = 5.4;
		int deletionAge = 90048;
		// insert everything we want our endresult to be in result, then put what we will delete in banadyMethod with
		// deletionGPA, deletionName, and deletionAge
		for(int i = 0;i<pageSize*5;i++)
		{
			int age =  uniqueAge[i];
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
			
			colData.clear();
			colData.put("id", new Integer(age));
			colData.put("age", new Integer(deletionAge));
			colData.put("name", new String(deletionName));
			colData.put("gpa", new Double(deletionGPA));
			database.insertIntoTable("banadyMethod", colData);
			
		}
		colData.clear();
		colData.put("gpa", new Double(deletionGPA));
		colData.put("name", new String(deletionName));
		colData.put("age", new Integer(deletionAge));
		database.deleteFromTable("banadyMethod", colData);
		
		assertTrue(banadyMethod.pageNames.size()==result.pageNames.size(),"Expected the number of pages to be " 
				    + banadyMethod.pageNames.size()	+ ", but instead got " +  result.pageNames.size());
		for(int i = 0;i<result.pageNames.size();i++)
		{
			Page banadyMethodPage = (Page) deserializeData(banadyMethod.filepath + banadyMethod.pageNames.get(i));
			Page resultPage = (Page) deserializeData(result.filepath + result.pageNames.get(i));
			assertTrue(comparePage(banadyMethodPage,resultPage), "\n Expected the tuples in page " + banadyMethodPage.name + " to be: \n"
					+ resultPage + "\n but instead got: \n" + banadyMethodPage);
		}
		
		
	}

	// This checks that deleting the tuple also deletes it from the
	// index (if it exists)
	@Test 
	@DisplayName("DeleteFromTable_WithIndexOnDouble_ShouldDeleteFromIndexToo")
	void Delete_Tuple_From_Double_Index_Too() throws ClassNotFoundException, DBAppException, IOException
	{
		database.createIndex("result","gpa", "gpaIndex");
		database.createIndex("banadyMethod","gpa", "gpaIndex");
		int index = insertRandomTuples(10);
		
		Index resultIndex = (Index) deserializeData(result.filepath+"/indices/" + "gpaIndex.ser");
		ArrayList<Vector<String>> resultPointers = resultIndex.searchGreaterThan(new Datatype(0.0), true);
		ArrayList<String> resultPointerNumbers = new ArrayList<String>();
		for(int i = 0;i<resultPointers.size();i++)
		{
			String page = resultPointers.get(i).get(0);
			resultPointerNumbers.add(page.substring(page.length(),page.length()));
		}
		
		for(int i =0;i<pageSize*3;i++)
		{
			int id =  uniqueID[++index];
			int age =  uniqueID[i];
			double gpa = 6.04;
			String name = randomString();
			colData.clear();
			colData.put("id", new Integer(id));
			colData.put("name", new String(name));
			colData.put("age", new Integer(age));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);

		}
		colData.clear();
		colData.put("gpa", new Double(6.04));
		database.deleteFromTable("banadyMethod", colData);
		
		Index banadyMethodIndex = (Index) deserializeData(banadyMethod.filepath+"/indices/" + "gpaIndex.ser");
		ArrayList<Vector<String>> banadyMethodPointers = banadyMethodIndex.searchGreaterThan(new Datatype(0.0), true);
		ArrayList<String> banadyMethodPointerNumbers = new ArrayList<String>();
		assertEquals(resultPointers.size(), banadyMethodPointers.size());
		for(int i = 0;i<resultPointers.size();i++)
		{
			String page = banadyMethodPointers.get(i).get(0);
			banadyMethodPointerNumbers.add(page.substring(page.length(),page.length()));
		}
		assertTrue(banadyMethodPointerNumbers.equals(resultPointerNumbers), 
				"Expected " + resultPointers + ", but instead got " + banadyMethodPointers);


	}
	
	@Test
	@DisplayName("DeleteFromTable_WithIndexOnString_ShouldDeleteFromIndexToo")
	void Delete_Tuple_From_String_Index_Too() throws ClassNotFoundException, DBAppException, IOException
	{
		database.createIndex("result","name", "nameIndex");
		database.createIndex("banadyMethod","name", "nameIndex");
		int index = insertRandomTuples(10);
		
		Index resultIndex = (Index) deserializeData(result.filepath+"/indices/" + "nameIndex.ser");
		ArrayList<Vector<String>> resultPointers = resultIndex.searchGreaterThan(new Datatype(""), true);
		ArrayList<String> resultPointerNumbers = new ArrayList<String>();
		for(int i = 0;i<resultPointers.size();i++)
		{
			String page = resultPointers.get(i).get(0);
			resultPointerNumbers.add(page.substring(page.length(),page.length()));
		}
		
		double [] uniqueGPA = random.doubles(5,10).distinct().mapToObj(d -> (double) Math.round(d * 1000) / 1000)
				.mapToDouble(Double::doubleValue).limit(pageSize*4).toArray();
		for(int i =0;i<pageSize*3;i++)
		{
			int id =  uniqueID[++index];
			int age =  uniqueID[i];
			double gpa = uniqueGPA[i];
			String name = "Aya";
			colData.clear();
			colData.put("id", new Integer(id));
			colData.put("name", new String(name));
			colData.put("age", age);
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
			
		}
		colData.clear();
		colData.put("name", "Aya");
		database.deleteFromTable("banadyMethod", colData);
		
		Index banadyMethodIndex = (Index) deserializeData(banadyMethod.filepath+"/indices/" + "nameIndex.ser");
		ArrayList<Vector<String>> banadyMethodPointers = banadyMethodIndex.searchGreaterThan(new Datatype(""), true);
		ArrayList<String> banadyMethodPointerNumbers = new ArrayList<String>();
		assertEquals(resultPointers.size(), banadyMethodPointers.size());
		for(int i = 0;i<resultPointers.size();i++)
		{
			String page = banadyMethodPointers.get(i).get(0);
			banadyMethodPointerNumbers.add(page.substring(page.length(),page.length()));
		}
		assertTrue(banadyMethodPointerNumbers.equals(resultPointerNumbers), 
				"Expected " + resultPointers + ", but instead got " + banadyMethodPointers);
		
		
	}
	
	
	@Test
	@DisplayName("DeleteFromTable_WithIndexOnInteger_ShouldDeleteFromIndexToo")
	void Delete_Tuple_From_Integer_Index_Too() throws ClassNotFoundException, DBAppException, IOException
	{
		database.createIndex("result","age", "ageIndex");
		database.createIndex("banadyMethod","age", "ageIndex");
		int index = insertRandomTuples(10);
		
		Index resultIndex = (Index) deserializeData(result.filepath+"/indices/" + "ageIndex.ser");
		ArrayList<Vector<String>> resultPointers = resultIndex.searchGreaterThan(new Datatype(-1), true);
		ArrayList<String> resultPointerNumbers = new ArrayList<String>();
		for(int i = 0;i<resultPointers.size();i++)
		{
			String page = resultPointers.get(i).get(0);
			resultPointerNumbers.add(page.substring(page.length(),page.length()));
		}
		
		double [] uniqueGPA = random.doubles(5,10).distinct().mapToObj(d -> (double) Math.round(d * 1000) / 1000)
				.mapToDouble(Double::doubleValue).limit(pageSize*4).toArray();
		for(int i =0;i<pageSize*3;i++)
		{
			int id =  uniqueID[++index];
			int age =  100002;
			double gpa = uniqueGPA[i];
			String name = randomString();
			colData.clear();
			colData.put("id", new Integer(id));
			colData.put("name", new String(name));
			colData.put("age", age);
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
			
		}
		colData.clear();
		colData.put("age", new Integer(100002));
		database.deleteFromTable("banadyMethod", colData);
		
		Index banadyMethodIndex = (Index) deserializeData(banadyMethod.filepath+"/indices/" + "ageIndex.ser");
		ArrayList<Vector<String>> banadyMethodPointers = banadyMethodIndex.searchGreaterThan(new Datatype(-1), true);
		ArrayList<String> banadyMethodPointerNumbers = new ArrayList<String>();
		assertEquals(resultPointers.size(), banadyMethodPointers.size());
		for(int i = 0;i<resultPointers.size();i++)
		{
			String page = banadyMethodPointers.get(i).get(0);
			banadyMethodPointerNumbers.add(page.substring(page.length(),page.length()));
		}
		assertTrue(banadyMethodPointerNumbers.equals(resultPointerNumbers), 
				"Expected " + resultPointers + ", but instead got " + banadyMethodPointers);
		
		
	}
	
	@RepeatedTest(value = 5)
	@DisplayName("DeleteFromTable_WithIndexOnIntegerAndDoubleAndString_ShouldDeleteFromAllThreeIndices")
	void DeleteFromAllIndices() throws ClassNotFoundException, DBAppException, IOException
	{
		database.createIndex("result","age", "ageIndex");
		database.createIndex("result","name", "nameIndex");
		database.createIndex("result","gpa", "gpaIndex");

		database.createIndex("banadyMethod","age", "ageIndex");
		database.createIndex("banadyMethod","name", "nameIndex");
		database.createIndex("banadyMethod","gpa", "gpaIndex");
		
		int index = insertRandomTuples(10);
		
		// resulting index should be as follows:
		Index resultAgeIndex = (Index) deserializeData(result.filepath+"/indices/" + "ageIndex.ser");
		Index resultNameIndex = (Index) deserializeData(result.filepath+"/indices/" + "nameIndex.ser");
		Index resultGpaIndex = (Index) deserializeData(result.filepath+"/indices/" + "gpaIndex.ser");
		
		ArrayList<Vector<String>> resultAgePointers = resultAgeIndex.searchGreaterThan(new Datatype(-1), true);
		ArrayList<Vector<String>> resultNamePointers = resultNameIndex.searchGreaterThan(new Datatype(""), true);
		ArrayList<Vector<String>> resultGpaPointers = resultGpaIndex.searchGreaterThan(new Datatype(0.0), true);
		
		
		ArrayList<String> resultAgePointerNumbers = new ArrayList<String>();
		ArrayList<String> resultNamePointerNumbers = new ArrayList<String>();
		ArrayList<String> resultGpaPointerNumbers = new ArrayList<String>();
		
		for(int i = 0;i<resultAgePointers.size();i++)
		{
			String page = resultAgePointers.get(i).get(0);
			resultAgePointerNumbers.add(page.substring(page.length(),page.length()));
			
			page = resultNamePointers.get(i).get(0);
			resultNamePointerNumbers.add(page.substring(page.length(),page.length()));

			page = resultGpaPointers.get(i).get(0);
			resultGpaPointerNumbers.add(page.substring(page.length(),page.length()));
		}
		
		int deletionAge = 100002;
		String deletionName = "Maryam";
		Double deletionGPA = 20.34;
		for(int i =0;i<pageSize*3;i++)
		{
			int id =  uniqueID[++index];

			colData.clear();
			colData.put("id", new Integer(id));
			colData.put("name", new String(deletionName));
			colData.put("age", new Integer(deletionAge));
			colData.put("gpa", new Double(deletionGPA));
			database.insertIntoTable("banadyMethod", colData);
			
		}
		
		colData.clear();
		colData.put("age", new Integer(deletionAge));
		colData.put("name", new String(deletionName));
		colData.put("gpa", new Double(deletionGPA));
		database.deleteFromTable("banadyMethod", colData);
		
		Index banadyMethodAgeIndex = (Index) deserializeData(banadyMethod.filepath+"/indices/" + "ageIndex.ser");
		Index banadyMethodNameIndex = (Index) deserializeData(banadyMethod.filepath+"/indices/" + "nameIndex.ser");
		Index banadyMethodGpaIndex = (Index) deserializeData(banadyMethod.filepath+"/indices/" + "gpaIndex.ser");
		
		ArrayList<Vector<String>> banadyMethodAgePointers = banadyMethodAgeIndex.searchGreaterThan(new Datatype(-1), true);
		ArrayList<Vector<String>> banadyMethodNamePointers = banadyMethodNameIndex.searchGreaterThan(new Datatype(""), true);
		ArrayList<Vector<String>> banadyMethodGpaPointers = banadyMethodGpaIndex.searchGreaterThan(new Datatype(0.0), true);
		
		
		ArrayList<String> banadyMethodAgePointerNumbers = new ArrayList<String>();
		ArrayList<String> banadyMethodNamePointerNumbers = new ArrayList<String>();
		ArrayList<String> banadyMethodGpaPointerNumbers = new ArrayList<String>();
		
		assertEquals(resultNamePointers.size(), banadyMethodNamePointers.size());
		assertEquals(resultGpaPointers.size(), banadyMethodGpaPointers.size());
		
		for(int i = 0;i<banadyMethodAgePointers.size();i++)
		{
			String page = banadyMethodAgePointers.get(i).get(0);
			banadyMethodAgePointerNumbers.add(page.substring(page.length(),page.length()));
			
			page = banadyMethodNamePointers.get(i).get(0);
			banadyMethodNamePointerNumbers.add(page.substring(page.length(),page.length()));

			page = banadyMethodGpaPointers.get(i).get(0);
			banadyMethodGpaPointerNumbers.add(page.substring(page.length(),page.length()));
		}
		assertTrue(banadyMethodAgePointerNumbers.equals(resultAgePointerNumbers), 
				"Expected " + resultAgePointerNumbers + ", but instead got " + banadyMethodAgePointers);
		assertTrue(banadyMethodNamePointerNumbers.equals(resultNamePointerNumbers), 
				"Expected " + resultNamePointerNumbers + ", but instead got " + banadyMethodAgePointers);
		assertTrue(banadyMethodGpaPointerNumbers.equals(resultGpaPointerNumbers), 
				"Expected " + resultGpaPointerNumbers + ", but instead got " + banadyMethodAgePointers);
	}
	
	
}
