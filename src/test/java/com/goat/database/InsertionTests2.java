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
import java.util.Collections;
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
public class InsertionTests2 {
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
		htbl.put("name", "java.lang.String");
		htbl.put("gpa", "java.lang.Double");

		colData = new Hashtable<String, Object>();



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

	private void insertRandomTuples(int numberOfPages) throws ClassNotFoundException, DBAppException, IOException
	{
		int [] uniqueID = random.ints(0,50000).distinct().limit(2000*numberOfPages).toArray();
		double [] uniqueGPA = random.doubles(0,29).distinct().mapToObj(d -> (double) Math.round(d * 1000) / 1000)
				.mapToDouble(Double::doubleValue).limit(pageSize*2000*numberOfPages).toArray();

		for(int i = 0 ;i<=numberOfPages*5;i++)
		{
			int id =  uniqueID[i];
			double gpa = uniqueGPA[i];
			String name = randomString();
			colData.clear();
			colData.put("id", new Integer(id));
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
			database.insertIntoTable("result", colData);

		}
	}

	//------------------------------------------TESTS---------------------------------------------------------

	@DisplayName("InsertTable_WithStringAsClusteringKey_ShouldHaveItSortedOnThatKey")
	@Test
	void insertIntoStringClusteringKey() throws ClassNotFoundException, DBAppException, IOException
	{
		database.createTable("banadyMethod", "name", htbl);	
		database.createTable("result", "name", htbl);	
		banadyMethod = database.tables.get(0);
		insertRandomTuples(5);

		// arraylist holds the tuples in the order the come from table
		ArrayList<String> tableTuples = new ArrayList<String>();
		for(int i =0;i<banadyMethod.pageNames.size();i++)
		{
			Page currPage = (Page) deserializeData(banadyMethod.filepath + banadyMethod.pageNames.get(i));
			ArrayList<String> pageTuples = new ArrayList<String>();
			for(Tuple currTuple:currPage.tuples)
			{
				tableTuples.add((String) currTuple.entry.get("name"));
				pageTuples.add((String) currTuple.entry.get("name"));
			}

			// check if this page on its own is sorted
			ArrayList<String> sortedPageTuples = new ArrayList<String>(pageTuples);
			Collections.sort(sortedPageTuples);
			assertTrue(pageTuples.equals(sortedPageTuples),"Expected " +sortedPageTuples + ", but instead got " + pageTuples);
		}
		// check if the entire table is sorted
		ArrayList<String> sortedTableTuples = new ArrayList<String>(tableTuples);
		Collections.sort(sortedTableTuples);
		assertTrue(tableTuples.equals(sortedTableTuples),"Expected " +sortedTableTuples + ", but instead got " + tableTuples);
	}

	@DisplayName("InsertTable_WithDoubleAsClusteringKey_ShouldHaveItSortedOnThatKey")
	@Test
	void insertIntoDoubleClusteringKey() throws ClassNotFoundException, DBAppException, IOException
	{
		database.createTable("banadyMethod", "gpa", htbl);
		database.createTable("result", "gpa", htbl);
		banadyMethod = database.tables.get(0);
		insertRandomTuples(6);

		// arraylist holds the tuples in the order the come from table
		ArrayList<Double> tableTuples = new ArrayList<Double>();
		for(int i =0;i<banadyMethod.pageNames.size();i++)
		{
			Page currPage = (Page) deserializeData(banadyMethod.filepath + banadyMethod.pageNames.get(i));
			ArrayList<Double> pageTuples = new ArrayList<Double>();
			for(Tuple currTuple:currPage.tuples)
			{
				tableTuples.add((Double) currTuple.entry.get("gpa"));
				pageTuples.add((Double) currTuple.entry.get("gpa"));
			}

			// check if this page on its own is sorted
			ArrayList<Double> sortedPageTuples = new ArrayList<Double>(pageTuples);
			Collections.sort(sortedPageTuples);
			assertTrue(pageTuples.equals(sortedPageTuples),"Expected " +sortedPageTuples + ", but instead got " + pageTuples);
		}
		// check if the entire table is sorted
		ArrayList<Double> sortedTableTuples = new ArrayList<Double>(tableTuples);
		Collections.sort(sortedTableTuples);
		assertTrue(tableTuples.equals(sortedTableTuples),"Expected " +sortedTableTuples + ", but instead got " + tableTuples);
	}

	@DisplayName("InsertTable_WithIndexOnInteger_ShouldUpdateIndexOnEveryInsert")
	@Test
	// hopefully test works? it inserts into two tables: one creates index after isnertions, and the other before
	// it then compares the indices of both tables and checks if they are the same
	void insertIntoIntegerIndex() throws ClassNotFoundException, DBAppException, IOException
	{
		database.createTable("banadyMethod", "id", htbl);
		database.createTable("result", "id", htbl);
		banadyMethod = database.tables.get(0);
		result = database.tables.get(1);

		database.createIndex("banadyMethod", "id", "idIndex");
		insertRandomTuples(7);
		database.createIndex("result", "id", "idIndex");


		Index resultIndex = (Index) deserializeData(result.filepath+"/indices/" + "idIndex.ser");
		Index banadyMethodIndex = (Index) deserializeData(banadyMethod.filepath+"/indices/" + "idIndex.ser");
		ArrayList<Vector<String>> resultPointers = resultIndex.searchGreaterThan(new Datatype(-1), true);
		ArrayList<Vector<String>> banadyMethodPointers = banadyMethodIndex.searchGreaterThan(new Datatype(-1), true);
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
		assertTrue(banadyMethodPointerNumbers.equals(resultPointerNumbers), "Expected " + resultPointers + ", but instead got " + banadyMethodPointers);
	}

	@DisplayName("InsertTable_WithIndexOnString_ShouldUpdateIndexOnEveryInsert")
	@Test
	// hopefully test works? it inserts into two tables: one creates index after isnertions, and the other before
	// it then compares the indices of both tables and checks if they are the same
	void insertIntoStringIndex() throws ClassNotFoundException, DBAppException, IOException
	{
		database.createTable("banadyMethod", "id", htbl);
		database.createTable("result", "id", htbl);
		banadyMethod = database.tables.get(0);
		result = database.tables.get(1);

		database.createIndex("banadyMethod", "name", "nameIndex");
		insertRandomTuples(7);
		database.createIndex("result", "name", "nameIndex");


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
		assertTrue(banadyMethodPointerNumbers.equals(resultPointerNumbers), "Expected " + resultPointers + ", but instead got " + banadyMethodPointers);
	}

	@DisplayName("InsertTable_WithIndexOnDouble_ShouldUpdateIndexOnEveryInsert")
	@Test
	// hopefully test works? it inserts into two tables: one creates index after isnertions, and the other before
	// it then compares the indices of both tables and checks if they are the same
	void insertIntoDoubleIndex() throws ClassNotFoundException, DBAppException, IOException
	{
		database.createTable("banadyMethod", "id", htbl);
		database.createTable("result", "id", htbl);
		banadyMethod = database.tables.get(0);
		result = database.tables.get(1);

		database.createIndex("banadyMethod", "gpa", "gpaIndex");
		insertRandomTuples(2);
		database.createIndex("result", "gpa", "gpaIndex");


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
		assertTrue(banadyMethodPointerNumbers.equals(resultPointerNumbers), "Expected " + resultPointers + ", but instead got " + banadyMethodPointers);
	}

	@RepeatedTest(value = 10)
	@DisplayName("InsertTable_WithIndexOnIntegerAndStringAndDouble_ShouldUpdateIndicesOnEveryInsert")
	void insertIntoMultipleIndices() throws ClassNotFoundException, DBAppException, IOException
	{
		database.createTable("banadyMethod", "id", htbl);
		database.createTable("result", "id", htbl);
		banadyMethod = database.tables.get(0);
		result = database.tables.get(1);

		database.createIndex("banadyMethod", "gpa", "gpaIndex");
		database.createIndex("banadyMethod", "name", "nameIndex");
		database.createIndex("banadyMethod", "id", "idIndex");
		insertRandomTuples(1);
		database.createIndex("result", "gpa", "gpaIndex");
		database.createIndex("result", "name", "nameIndex");
		database.createIndex("result", "id", "idIndex");

		// check gpa index tmam
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
		assertTrue(banadyMethodPointerNumbers.equals(resultPointerNumbers), "Expected " + resultPointers + ", but instead got " + banadyMethodPointers);

		// check name index tmam
		resultIndex = (Index) deserializeData(result.filepath+"/indices/" + "nameIndex.ser");
		banadyMethodIndex = (Index) deserializeData(banadyMethod.filepath+"/indices/" + "nameIndex.ser");
		resultPointers = resultIndex.searchGreaterThan(new Datatype(""), true);
		banadyMethodPointers = banadyMethodIndex.searchGreaterThan(new Datatype(""), true);
		assertEquals(resultPointers.size(), banadyMethodPointers.size());

		resultPointerNumbers = new ArrayList<String>();
		banadyMethodPointerNumbers = new ArrayList<String>();
		for(int i = 0;i<resultPointers.size();i++)
		{
			String page = resultPointers.get(i).get(0);
			resultPointerNumbers.add(page.substring(page.length(),page.length()));

			page = banadyMethodPointers.get(i).get(0);
			banadyMethodPointerNumbers.add(page.substring(page.length(),page.length()));
		}
		assertTrue(banadyMethodPointerNumbers.equals(resultPointerNumbers), "Expected " + resultPointers + ", but instead got " + banadyMethodPointers);

		// check id index tmam
		resultIndex = (Index) deserializeData(result.filepath+"/indices/" + "idIndex.ser");
		banadyMethodIndex = (Index) deserializeData(banadyMethod.filepath+"/indices/" + "idIndex.ser");
		resultPointers = resultIndex.searchGreaterThan(new Datatype(-1), true);
		banadyMethodPointers = banadyMethodIndex.searchGreaterThan(new Datatype(-1), true);
		assertEquals(resultPointers.size(), banadyMethodPointers.size());
		resultPointerNumbers = new ArrayList<String>();
		banadyMethodPointerNumbers = new ArrayList<String>();
		for(int i = 0;i<resultPointers.size();i++)
		{
			String page = resultPointers.get(i).get(0);
			resultPointerNumbers.add(page.substring(page.length(),page.length()));

			page = banadyMethodPointers.get(i).get(0);
			banadyMethodPointerNumbers.add(page.substring(page.length(),page.length()));
		}
		assertTrue(banadyMethodPointerNumbers.equals(resultPointerNumbers), "Expected " + resultPointers + ", but instead got " + banadyMethodPointers);
	}
}

