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
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

// TODO THE GREATER THAN DOES NOT TEST EQUAL; TEST THIS MANUALLY
@SuppressWarnings("deprecation")
public class SelectionTests 
{
	DBApp database;
	Table banadyMethod;
	Hashtable<String,String> htbl;
	Hashtable<String,Object> colData;
	int[] uniqueID;
	int pageSize;
	Random random;
	SQLTerm[] arrSqlTerms;
	ArrayList<Hashtable<String,Object>> expectedResult;
	ArrayList<Hashtable<String,Object>> selectionResult;


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
		colData = new Hashtable<String, Object>();
		expectedResult = new ArrayList<Hashtable<String,Object>>();
		selectionResult = new ArrayList<Hashtable<String,Object>>();
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

	private int insertRandomTuples(int numberOfPages) throws ClassNotFoundException, DBAppException, IOException
	{
		uniqueID = random.ints(0,5000).distinct().limit(pageSize*2*numberOfPages).toArray();
		int[] uniqueAge = random.ints(100,500).distinct().limit(pageSize*2*numberOfPages).toArray();
		double [] uniqueGPA = random.doubles(3,5).distinct().mapToObj(d -> (double) Math.round(d * 1000) / 1000)
				.mapToDouble(Double::doubleValue).limit(pageSize*2*numberOfPages).toArray();

		for(int i = 0 ;i<pageSize*numberOfPages;i++)
		{
			int id =  uniqueID[i];
			int age =  uniqueAge[i];
			double gpa = uniqueGPA[i];
			String name = randomString();
			colData.clear();
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(age));
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
		}
		colData.clear();

		return pageSize*numberOfPages;
	}

	private int insertRandomTuplesAndSaveInResult(int numberOfPages) throws ClassNotFoundException, DBAppException, IOException
	{

		uniqueID = random.ints(0,5000).distinct().limit(pageSize*2*numberOfPages).toArray();
		int[] uniqueAge = random.ints(100,500).distinct().limit(pageSize*2*numberOfPages).toArray();
		double [] uniqueGPA = random.doubles(3,5).distinct().mapToObj(d -> (double) Math.round(d * 1000) / 1000)
				.mapToDouble(Double::doubleValue).limit(pageSize*2*numberOfPages).toArray();

		for(int i = 0 ;i<pageSize*numberOfPages;i++)
		{
			int id =  uniqueID[i];
			int age =  uniqueAge[i];
			double gpa = uniqueGPA[i];
			String name = randomString();
			colData = new Hashtable<String, Object>();
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(age));
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
			expectedResult.add(colData);
		}
		colData.clear();

		return pageSize*numberOfPages;


	}
	// ----------------------------------------------------------------------------------------------------TESTS----------------------------------------------------------------------------------------------------------
	/*
	 *  the following should be true for every test: (this excludes any manual inserts not done by insertRandomTuples())
	 *  id is a random id from 0 to 5000
	 *  age ranges from 100 to 500
	 *  name is a random 10 character string with no spaces
	 *  gpa is from 3.000 to 5.000 (exclusive)
	 */


	@DisplayName("SelectFromTable_UsingEqualOnIntegerClusteringKey_ShouldReturnMultipleTuples")
	@Test
	public void SelectWithEqualClusteringKey() throws ClassNotFoundException, DBAppException, IOException
	{
		int idIndex = insertRandomTuples(10);

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "id";
		arrSqlTerms[0]._strOperator = "=";
		arrSqlTerms[0]._objValue = new Integer(uniqueID[idIndex]);

		colData.put("id", new Integer(uniqueID[idIndex]));
		colData.put("age", new Integer(20));
		colData.put("name", new String("owo"));
		colData.put("gpa", new Double(1.2));
		database.insertIntoTable("banadyMethod", colData);

		Iterator<Tuple> iterate = database.selectFromTable(arrSqlTerms, new String[0]);

		assertTrue(iterate.hasNext(),"Expected for iterator to have an element, but found nothing");
		Tuple result = (Tuple) iterate.next();

		assertTrue(colData.equals(result.entry), "Expected " + colData + ", but instead got " + result.entry);
	}

	// integer select tests
	@DisplayName("SelectFromTable_UsingEqualOnIntegerWithNoIndex_ShouldReturnMultipleTuples")
	@Test
	public void SelectWithEqualIntegerNoIndex() throws ClassNotFoundException, DBAppException, IOException
	{
		int idIndex = insertRandomTuples(10);
		int selectionAge = 23;

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "age";
		arrSqlTerms[0]._strOperator = "=";
		arrSqlTerms[0]._objValue = new Integer(selectionAge);

		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 100) / 100)
				.mapToDouble(Double::doubleValue).limit(pageSize*5).toArray();
		for(int i = 0;i<pageSize*3;i++)
		{
			int id = uniqueID[idIndex++];		
			String name = randomString();
			Double gpa = uniqueGPA[i];
			colData = new Hashtable<String, Object>();
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(selectionAge));
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
			expectedResult.add(colData);
		}


		Iterator<Tuple>iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);

		int iteratorSize = 0;
		while(iteratorSelectionResult.hasNext())
		{
			Tuple currTuple =  iteratorSelectionResult.next();
			iteratorSize++;
			selectionResult.add(currTuple.entry);
		}

		assertEquals(iteratorSize,expectedResult.size());
		for(int i = 0;i<iteratorSize;i++)
		{
			assertTrue(selectionResult.contains(expectedResult.get(i)), "Did not find " + expectedResult.get(i) + "in"
					+ " the selection result");
		}
	}

	@DisplayName("SelectFromTable_UsingEqualOnIntegerWithIndex_ShouldReturnMultipleTuples")
	@Test
	public void SelectWithEqualIntegerIndex() throws ClassNotFoundException, DBAppException, IOException
	{
		int idIndex = insertRandomTuples(10);
		database.createIndex("banadyMethod", "age", "ageIndex");
		int selectionAge = 23;

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "age";
		arrSqlTerms[0]._strOperator = "=";
		arrSqlTerms[0]._objValue = new Integer(selectionAge);

		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 100) / 100)
				.mapToDouble(Double::doubleValue).limit(pageSize*5).toArray();
		for(int i = 0;i<pageSize*3;i++)
		{
			int id = uniqueID[idIndex++];		
			String name = randomString();
			Double gpa = uniqueGPA[i];
			colData = new Hashtable<String, Object>();
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(selectionAge));
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
			expectedResult.add(colData);
		}


		Iterator<Tuple>iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);

		int iteratorSize = 0;
		while(iteratorSelectionResult.hasNext())
		{
			Tuple currTuple =  iteratorSelectionResult.next();
			iteratorSize++;
			selectionResult.add(currTuple.entry);
		}

		assertEquals(iteratorSize,expectedResult.size());
		for(int i = 0;i<iteratorSize;i++)
		{
			assertTrue(selectionResult.contains(expectedResult.get(i)), "Did not find " + expectedResult.get(i) + "in"
					+ " the selection result");
		}
	}

	@DisplayName("SelectFromTable_UsingNotEqualOnIntegerWithNoIndex_ShouldReturnMultipleTuples")
	@Test
	public void SelectWithNotEqualIntegerNoIndex() throws ClassNotFoundException, DBAppException, IOException
	{
		int idIndex = insertRandomTuplesAndSaveInResult(10);
		int selectionAge = 23;

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "age";
		arrSqlTerms[0]._strOperator = "!=";
		arrSqlTerms[0]._objValue = new Integer(selectionAge);

		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 100) / 100)
				.mapToDouble(Double::doubleValue).limit(pageSize*5).toArray();
		for(int i = 0;i<pageSize*3;i++)
		{
			int id = uniqueID[idIndex++];		
			String name = randomString();
			Double gpa = uniqueGPA[i];
			colData = new Hashtable<String, Object>();
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(selectionAge));
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
		}


		Iterator<Tuple>iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);

		int iteratorSize = 0;
		while(iteratorSelectionResult.hasNext())
		{
			Tuple currTuple =  iteratorSelectionResult.next();
			iteratorSize++;
			selectionResult.add(currTuple.entry);
		}

		assertEquals(iteratorSize,expectedResult.size());
		for(int i = 0;i<iteratorSize;i++)
		{
			assertTrue(selectionResult.contains(expectedResult.get(i)), "Did not find " + expectedResult.get(i) + "in"
					+ " the selection result");
		}
	}

	@DisplayName("SelectFromTable_UsingNotEqualOnIntegerWithIndex_ShouldReturnMultipleTuples")
	@Test
	public void SelectWithNotEqualIntegerIndex() throws ClassNotFoundException, DBAppException, IOException
	{
		int idIndex = insertRandomTuplesAndSaveInResult(10);
		database.createIndex("banadyMethod", "age", "ageIndex");
		int selectionAge = 23;

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "age";
		arrSqlTerms[0]._strOperator = "!=";
		arrSqlTerms[0]._objValue = new Integer(selectionAge);

		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 100) / 100)
				.mapToDouble(Double::doubleValue).limit(pageSize*5).toArray();
		for(int i = 0;i<pageSize*3;i++)
		{
			int id = uniqueID[idIndex++];		
			String name = randomString();
			Double gpa = uniqueGPA[i];
			colData = new Hashtable<String, Object>();
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(selectionAge));
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
		}

		Iterator<Tuple>iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);

		int iteratorSize = 0;
		while(iteratorSelectionResult.hasNext())
		{
			Tuple currTuple =  iteratorSelectionResult.next();
			iteratorSize++;
			selectionResult.add(currTuple.entry);
		}

		assertEquals(iteratorSize,expectedResult.size());
		for(int i = 0;i<iteratorSize;i++)
		{
			assertTrue(selectionResult.contains(expectedResult.get(i)), "Did not find " + expectedResult.get(i) + "in"
					+ " the selection result");
		}
	}

	@DisplayName("SelectFromTable_UsingGreaterThanIntegerWithNoIndex_ShouldReturnMultipleTuples")
	@Test
	public void SelectWithGreaterThanIntegerNoIndex() throws ClassNotFoundException, DBAppException, IOException
	{
		int idIndex = insertRandomTuples(10);

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "age";
		arrSqlTerms[0]._strOperator = ">";
		arrSqlTerms[0]._objValue = new Integer(500);

		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 100) / 100)
				.mapToDouble(Double::doubleValue).limit(pageSize*5).toArray();
		int[] uniqueSelectionAge = random.ints(501,1000).distinct().limit(pageSize*10).toArray();

		for(int i = 0;i<pageSize*3;i++)
		{
			int id = uniqueID[idIndex++];
			int selectionAge = uniqueSelectionAge[i];
			String name = randomString();
			Double gpa = uniqueGPA[i];
			colData = new Hashtable<String, Object>();
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(selectionAge));
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
			expectedResult.add(colData);
		}

		Iterator<Tuple>iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);

		int iteratorSize = 0;
		while(iteratorSelectionResult.hasNext())
		{
			Tuple currTuple =  iteratorSelectionResult.next();
			iteratorSize++;
			selectionResult.add(currTuple.entry);
		}

		assertEquals(iteratorSize,expectedResult.size());
		for(int i = 0;i<iteratorSize;i++)
		{
			assertTrue(selectionResult.contains(expectedResult.get(i)), "Did not find " + expectedResult.get(i) + "in"
					+ " the selection result");
		}
	}

	@DisplayName("SelectFromTable_UsingGreaterThanIntegerWithIndex_ShouldReturnMultipleTuples")
	@Test
	public void SelectWithGreaterThanIntegerIndex() throws ClassNotFoundException, DBAppException, IOException
	{
		int idIndex = insertRandomTuples(10);
		database.createIndex("banadyMethod", "age", "ageIndex");


		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "age";
		arrSqlTerms[0]._strOperator = ">";
		arrSqlTerms[0]._objValue = new Integer(500);

		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 100) / 100)
				.mapToDouble(Double::doubleValue).limit(pageSize*5).toArray();
		int[] uniqueSelectionAge = random.ints(501,1000).distinct().limit(pageSize*10).toArray();

		for(int i = 0;i<pageSize*3;i++)
		{
			int id = uniqueID[idIndex++];
			int selectionAge = uniqueSelectionAge[i];
			String name = randomString();
			Double gpa = uniqueGPA[i];
			colData = new Hashtable<String, Object>();
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(selectionAge));
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
			expectedResult.add(colData);
		}

		Iterator<Tuple>iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);

		int iteratorSize = 0;
		while(iteratorSelectionResult.hasNext())
		{
			Tuple currTuple =  iteratorSelectionResult.next();
			iteratorSize++;
			selectionResult.add(currTuple.entry);
		}

		assertEquals(iteratorSize,expectedResult.size());
		for(int i = 0;i<iteratorSize;i++)
		{
			assertTrue(selectionResult.contains(expectedResult.get(i)), "Did not find " + expectedResult.get(i) + "in"
					+ " the selection result");
		}
	}

	@DisplayName("SelectFromTable_UsingLessThanIntegerWithNoIndex_ShouldReturnMultipleTuples")
	@Test
	public void SelectWithLessThanIntegerNoIndex() throws ClassNotFoundException, DBAppException, IOException
	{
		int idIndex = insertRandomTuples(10);
		database.createIndex("banadyMethod", "age", "ageIndex");

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "age";
		arrSqlTerms[0]._strOperator = "<";
		arrSqlTerms[0]._objValue = new Integer(100);

		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 100) / 100)
				.mapToDouble(Double::doubleValue).limit(pageSize*5).toArray();
		int[] uniqueSelectionAge = random.ints(0,100).distinct().limit(pageSize*10).toArray();

		for(int i = 0;i<pageSize*3;i++)
		{
			int id = uniqueID[idIndex++];
			int selectionAge = uniqueSelectionAge[i];
			String name = randomString();
			Double gpa = uniqueGPA[i];
			colData = new Hashtable<String, Object>();
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(selectionAge));
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
			expectedResult.add(colData);
		}

		Iterator<Tuple>iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);

		int iteratorSize = 0;
		while(iteratorSelectionResult.hasNext())
		{
			Tuple currTuple =  iteratorSelectionResult.next();
			iteratorSize++;
			selectionResult.add(currTuple.entry);
		}

		assertEquals(iteratorSize,expectedResult.size());
		for(int i = 0;i<iteratorSize;i++)
		{
			assertTrue(selectionResult.contains(expectedResult.get(i)), "Did not find " + expectedResult.get(i) + "in"
					+ " the selection result");
		}
	}

	@DisplayName("SelectFromTable_UsingLessThanIntegerWithIndex_ShouldReturnMultipleTuples")
	@Test
	public void SelectWithLessThanIntegerIndex() throws ClassNotFoundException, DBAppException, IOException
	{
		int idIndex = insertRandomTuples(10);
		database.createIndex("banadyMethod", "age", "ageIndex");

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "age";
		arrSqlTerms[0]._strOperator = "<";
		arrSqlTerms[0]._objValue = new Integer(100);

		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 100) / 100)
				.mapToDouble(Double::doubleValue).limit(pageSize*5).toArray();
		int[] uniqueSelectionAge = random.ints(0,100).distinct().limit(pageSize*10).toArray();

		for(int i = 0;i<pageSize*3;i++)
		{
			int id = uniqueID[idIndex++];
			int selectionAge = uniqueSelectionAge[i];
			String name = randomString();
			Double gpa = uniqueGPA[i];
			colData = new Hashtable<String, Object>();
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(selectionAge));
			colData.put("name", new String(name));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
			expectedResult.add(colData);
		}

		Iterator<Tuple>iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);

		int iteratorSize = 0;
		while(iteratorSelectionResult.hasNext())
		{
			Tuple currTuple =  iteratorSelectionResult.next();
			iteratorSize++;
			selectionResult.add(currTuple.entry);
		}

		assertEquals(iteratorSize,expectedResult.size());
		for(int i = 0;i<iteratorSize;i++)
		{
			assertTrue(selectionResult.contains(expectedResult.get(i)), "Did not find " + expectedResult.get(i) + "in"
					+ " the selection result");
		}
	}

	// i am going aganist every testing convention and testing 6 different things here 3ashan mekasel ha3mel 
	// 6 methods kamn
	@DisplayName("SelectFromTable_WithNothingMeetingConditionIntegerNoIndex_ShouldReturnEmptyIterator")
	@Test
	public void SelectWithUnMetConditionIntegerNoIndex() throws ClassNotFoundException, DBAppException, IOException 
	{
		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "age";
		arrSqlTerms[0]._strOperator = "!=";
		arrSqlTerms[0]._objValue = new Integer(5);
		Iterator<Tuple>iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for != operator");

		insertRandomTuples(20);

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "age";
		arrSqlTerms[0]._strOperator = "=";
		arrSqlTerms[0]._objValue = new Integer(5);
		iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for = operator");

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "age";
		arrSqlTerms[0]._strOperator = ">";
		arrSqlTerms[0]._objValue = new Integer(50000);	
		iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for > operator");

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "age";
		arrSqlTerms[0]._strOperator = ">=";
		arrSqlTerms[0]._objValue = new Integer(50000);		
		iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for >= operator");

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "age";
		arrSqlTerms[0]._strOperator = "<";
		arrSqlTerms[0]._objValue = new Integer(90);

		iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for < operator");
		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "age";
		arrSqlTerms[0]._strOperator = "<=";
		arrSqlTerms[0]._objValue = new Integer(90);

		iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for <= operator");

	}

	@DisplayName("SelectFromTable_WithNothingMeetingConditionIntegerIndex_ShouldReturnEmptyIterator")
	@Test
	public void SelectWithUnMetConditionIntegerIndex() throws ClassNotFoundException, DBAppException, IOException 
	{
		database.createIndex("banadyMethod", "age", "ageIndex");

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "age";
		arrSqlTerms[0]._strOperator = "!=";
		arrSqlTerms[0]._objValue = new Integer(5);
		Iterator<Tuple>iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for != operator");

		insertRandomTuples(20);

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "age";
		arrSqlTerms[0]._strOperator = "=";
		arrSqlTerms[0]._objValue = new Integer(5);
		iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for = operator");

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "age";
		arrSqlTerms[0]._strOperator = ">";
		arrSqlTerms[0]._objValue = new Integer(50000);	
		iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for > operator");

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "age";
		arrSqlTerms[0]._strOperator = ">=";
		arrSqlTerms[0]._objValue = new Integer(50000);		
		iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for >= operator");

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "age";
		arrSqlTerms[0]._strOperator = "<";
		arrSqlTerms[0]._objValue = new Integer(90);

		iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for < operator");
		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "age";
		arrSqlTerms[0]._strOperator = "<=";
		arrSqlTerms[0]._objValue = new Integer(90);

		iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for <= operator");

	}
	// string select tests
	@DisplayName("SelectFromTable_UsingEqualOnStringWithNoIndex_ShouldReturnMultipleTuples")
	@Test
	public void SelectWithEqualStringNoIndex() throws ClassNotFoundException, DBAppException, IOException
	{
		int idIndex = insertRandomTuples(10);
		String selectionName = "Zeina";

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "name";
		arrSqlTerms[0]._strOperator = "=";
		arrSqlTerms[0]._objValue = new String(selectionName);

		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 100) / 100)
				.mapToDouble(Double::doubleValue).limit(pageSize*5).toArray();
		int [] uniqueAge = random.ints(0,200).distinct().limit(pageSize*7).toArray();
		for(int i = 0;i<pageSize*3;i++)
		{
			int id = uniqueID[idIndex++];
			int age = uniqueAge[i];
			Double gpa = uniqueGPA[i];
			colData = new Hashtable<String, Object>();
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(age));
			colData.put("name", new String(selectionName));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
			expectedResult.add(colData);
		}


		Iterator<Tuple>iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);

		int iteratorSize = 0;
		while(iteratorSelectionResult.hasNext())
		{
			Tuple currTuple =  iteratorSelectionResult.next();
			iteratorSize++;
			selectionResult.add(currTuple.entry);
		}

		assertEquals(iteratorSize,expectedResult.size());
		for(int i = 0;i<iteratorSize;i++)
		{
			assertTrue(selectionResult.contains(expectedResult.get(i)), "Did not find " + expectedResult.get(i) + "in"
					+ " the selection result");
		}
	}

	@DisplayName("SelectFromTable_UsingEqualOnStringWithIndex_ShouldReturnMultipleTuples")
	@Test
	public void SelectWithEqualStringIndex() throws ClassNotFoundException, DBAppException, IOException
	{
		int idIndex = insertRandomTuples(10);
		database.createIndex("banadyMethod", "name", "nameIndex");
		String selectionName = "Mo3taz";

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "name";
		arrSqlTerms[0]._strOperator = "=";
		arrSqlTerms[0]._objValue = new String(selectionName);

		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 100) / 100)
				.mapToDouble(Double::doubleValue).limit(pageSize*5).toArray();
		int [] uniqueAge = random.ints(0,200).distinct().limit(pageSize*7).toArray();
		for(int i = 0;i<pageSize*3;i++)
		{
			int id = uniqueID[idIndex++];
			int age = uniqueAge[i];
			Double gpa = uniqueGPA[i];
			colData = new Hashtable<String, Object>();
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(age));
			colData.put("name", new String(selectionName));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
			expectedResult.add(colData);
		}


		Iterator<Tuple>iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);

		int iteratorSize = 0;
		while(iteratorSelectionResult.hasNext())
		{
			Tuple currTuple =  iteratorSelectionResult.next();
			iteratorSize++;
			selectionResult.add(currTuple.entry);
		}

		assertEquals(iteratorSize,expectedResult.size());
		for(int i = 0;i<iteratorSize;i++)
		{
			assertTrue(selectionResult.contains(expectedResult.get(i)), "Did not find " + expectedResult.get(i) + "in"
					+ " the selection result");
		}
	}

	@DisplayName("SelectFromTable_UsingNotEqualOnStringWithNoIndex_ShouldReturnMultipleTuples")
	@Test
	public void SelectWithNotEqualStringNoIndex() throws ClassNotFoundException, DBAppException, IOException
	{
		int idIndex = insertRandomTuplesAndSaveInResult(10);
		String selectionName = "Rana";

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "name";
		arrSqlTerms[0]._strOperator = "!=";
		arrSqlTerms[0]._objValue = new String(selectionName);

		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 100) / 100)
				.mapToDouble(Double::doubleValue).limit(pageSize*5).toArray();
		int [] uniqueAge = random.ints(0,200).distinct().limit(pageSize*7).toArray();
		for(int i = 0;i<pageSize*3;i++)
		{
			int id = uniqueID[idIndex++];
			int age = uniqueAge[i];
			Double gpa = uniqueGPA[i];
			colData = new Hashtable<String, Object>();
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(age));
			colData.put("name", new String(selectionName));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
		}

		Iterator<Tuple>iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);

		int iteratorSize = 0;
		while(iteratorSelectionResult.hasNext())
		{
			Tuple currTuple =  iteratorSelectionResult.next();
			iteratorSize++;
			selectionResult.add(currTuple.entry);
		}

		assertEquals(iteratorSize,expectedResult.size());
		for(int i = 0;i<iteratorSize;i++)
		{
			assertTrue(selectionResult.contains(expectedResult.get(i)), "Did not find " + expectedResult.get(i) + "in"
					+ " the selection result");
		}
	}

	@DisplayName("SelectFromTable_UsingNotEqualOnStringWithIndex_ShouldReturnMultipleTuples")
	@Test
	public void SelectWithNotEqualStringIndex() throws ClassNotFoundException, DBAppException, IOException
	{
		int idIndex = insertRandomTuplesAndSaveInResult(10);
		database.createIndex("banadyMethod", "name", "nameIndex");
		String selectionName = "Mohamed";

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "name";
		arrSqlTerms[0]._strOperator = "!=";
		arrSqlTerms[0]._objValue = new String(selectionName);

		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 100) / 100)
				.mapToDouble(Double::doubleValue).limit(pageSize*5).toArray();
		int [] uniqueAge = random.ints(0,200).distinct().limit(pageSize*7).toArray();
		for(int i = 0;i<pageSize*3;i++)
		{
			int id = uniqueID[idIndex++];
			int age = uniqueAge[i];
			Double gpa = uniqueGPA[i];
			colData = new Hashtable<String, Object>();
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(age));
			colData.put("name", new String(selectionName));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
		}

		Iterator<Tuple>iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);

		int iteratorSize = 0;
		while(iteratorSelectionResult.hasNext())
		{
			Tuple currTuple =  iteratorSelectionResult.next();
			iteratorSize++;
			selectionResult.add(currTuple.entry);
		}

		assertEquals(iteratorSize,expectedResult.size());
		for(int i = 0;i<iteratorSize;i++)
		{
			assertTrue(selectionResult.contains(expectedResult.get(i)), "Did not find " + expectedResult.get(i) + "in"
					+ " the selection result");
		}
	}

	@DisplayName("SelectFromTable_UsingGreaterThanStringWithNoIndex_ShouldReturnMultipleTuples")
	@Test
	public void SelectWithGreaterThanStringNoIndex() throws ClassNotFoundException, DBAppException, IOException
	{
		int idIndex = insertRandomTuples(10);

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "name";
		arrSqlTerms[0]._strOperator = ">";
		arrSqlTerms[0]._objValue = new String("zzzzzzzzzz"); // did u know z lower case is bigger than Z uppercase

		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 100) / 100)
				.mapToDouble(Double::doubleValue).limit(pageSize*5).toArray();
		int [] uniqueAge = random.ints(0,200).distinct().limit(pageSize*7).toArray();
		for(int i = 0;i<pageSize*3;i++)
		{
			int id = uniqueID[idIndex++];
			int age = uniqueAge[i];
			Double gpa = uniqueGPA[i];
			colData = new Hashtable<String, Object>();
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(age));
			colData.put("name", new String(randomString() + " el masry"));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
			expectedResult.add(colData);
		}

		Iterator<Tuple>iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);

		int iteratorSize = 0;
		while(iteratorSelectionResult.hasNext())
		{
			Tuple currTuple =  iteratorSelectionResult.next();
			iteratorSize++;
			selectionResult.add(currTuple.entry);
		}

		assertEquals(iteratorSize,expectedResult.size());
		for(int i = 0;i<iteratorSize;i++)
		{
			assertTrue(selectionResult.contains(expectedResult.get(i)), "Did not find " + expectedResult.get(i) + "in"
					+ " the selection result");
		}
	}

	@DisplayName("SelectFromTable_UsingGreaterThanStringWithIndex_ShouldReturnMultipleTuples")
	@Test
	public void SelectWithGreaterThanStringIndex() throws ClassNotFoundException, DBAppException, IOException
	{
		int idIndex = insertRandomTuples(10);
		database.createIndex("banadyMethod", "name", "nameIndex");

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "name";
		arrSqlTerms[0]._strOperator = ">";
		arrSqlTerms[0]._objValue = new String("zzzzzzzzzz"); // did u know z lower case is bigger than Z uppercase

		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 100) / 100)
				.mapToDouble(Double::doubleValue).limit(pageSize*5).toArray();
		int [] uniqueAge = random.ints(0,200).distinct().limit(pageSize*7).toArray();
		for(int i = 0;i<pageSize*3;i++)
		{
			int id = uniqueID[idIndex++];
			int age = uniqueAge[i];
			Double gpa = uniqueGPA[i];
			colData = new Hashtable<String, Object>();
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(age));
			colData.put("name", new String(randomString() + " el masry"));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
			expectedResult.add(colData);
		}

		Iterator<Tuple>iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);

		int iteratorSize = 0;
		while(iteratorSelectionResult.hasNext())
		{
			Tuple currTuple =  iteratorSelectionResult.next();
			iteratorSize++;
			selectionResult.add(currTuple.entry);
		}

		assertEquals(iteratorSize,expectedResult.size());
		for(int i = 0;i<iteratorSize;i++)
		{
			assertTrue(selectionResult.contains(expectedResult.get(i)), "Did not find " + expectedResult.get(i) + "in"
					+ " the selection result");
		}
	}

	@DisplayName("SelectFromTable_UsingLessThanStringWithNoIndex_ShouldReturnMultipleTuples")
	@Test
	public void SelectWithLessThanStringNoIndex() throws ClassNotFoundException, DBAppException, IOException
	{
		int idIndex = insertRandomTuples(10);

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "name";
		arrSqlTerms[0]._strOperator = "<";
		arrSqlTerms[0]._objValue = new String("zzzzzzzzzz"); // did u know z lower case is bigger than Z uppercase

		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 100) / 100)
				.mapToDouble(Double::doubleValue).limit(pageSize*5).toArray();
		int [] uniqueAge = random.ints(0,200).distinct().limit(pageSize*7).toArray();
		for(int i = 0;i<pageSize*3;i++)
		{
			int id = uniqueID[idIndex++];
			int age = uniqueAge[i];
			Double gpa = uniqueGPA[i];
			colData = new Hashtable<String, Object>();
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(age));
			colData.put("name", new String(randomString().substring(0,5)));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
			expectedResult.add(colData);
		}

		Iterator<Tuple>iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);

		int iteratorSize = 0;
		while(iteratorSelectionResult.hasNext())
		{
			Tuple currTuple =  iteratorSelectionResult.next();
			iteratorSize++;
			selectionResult.add(currTuple.entry);
		}

		assertEquals(iteratorSize,expectedResult.size());
		for(int i = 0;i<iteratorSize;i++)
		{
			assertTrue(selectionResult.contains(expectedResult.get(i)), "Did not find " + expectedResult.get(i) + "in"
					+ " the selection result");
		}
	}

	@DisplayName("SelectFromTable_UsingLessThanStringWithIndex_ShouldReturnMultipleTuples")
	@Test
	public void SelectWithLessThanStringIndex() throws ClassNotFoundException, DBAppException, IOException
	{
		int idIndex = insertRandomTuples(10);
		database.createIndex("banadyMethod", "age", "ageIndex");

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "name";
		arrSqlTerms[0]._strOperator = "<";
		arrSqlTerms[0]._objValue = new String("zzzzzzzzzz"); // did u know z lower case is bigger than Z uppercase

		double [] uniqueGPA = random.doubles(0,5).distinct().mapToObj(d -> (double) Math.round(d * 100) / 100)
				.mapToDouble(Double::doubleValue).limit(pageSize*5).toArray();
		int [] uniqueAge = random.ints(0,200).distinct().limit(pageSize*7).toArray();
		for(int i = 0;i<pageSize*3;i++)
		{
			int id = uniqueID[idIndex++];
			int age = uniqueAge[i];
			Double gpa = uniqueGPA[i];
			colData = new Hashtable<String, Object>();
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(age));
			colData.put("name", new String(randomString().substring(0,5)));
			colData.put("gpa", new Double(gpa));
			database.insertIntoTable("banadyMethod", colData);
			expectedResult.add(colData);
		}

		Iterator<Tuple>iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);

		int iteratorSize = 0;
		while(iteratorSelectionResult.hasNext())
		{
			Tuple currTuple =  iteratorSelectionResult.next();
			iteratorSize++;
			selectionResult.add(currTuple.entry);
		}

		assertEquals(iteratorSize,expectedResult.size());
		for(int i = 0;i<iteratorSize;i++)
		{
			assertTrue(selectionResult.contains(expectedResult.get(i)), "Did not find " + expectedResult.get(i) + "in"
					+ " the selection result");
		}
	}

	@DisplayName("SelectFromTable_WithNothingMeetingConditionStringNoIndex_ShouldReturnEmptyIterator")
	@Test
	public void SelectWithUnMetConditionStringNoIndex() throws ClassNotFoundException, DBAppException, IOException 
	{
		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "name";
		arrSqlTerms[0]._strOperator = "!=";
		arrSqlTerms[0]._objValue = new String("nefsy 7ad yerood 3ala el dms bet3aty");
		Iterator<Tuple>iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for != operator");

		insertRandomTuples(20);

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "name";
		arrSqlTerms[0]._strOperator = "=";
		arrSqlTerms[0]._objValue = new String("bet5onak");
		iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for = operator");

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "name";
		arrSqlTerms[0]._strOperator = ">";
		arrSqlTerms[0]._objValue = new String("dana string kebeer fas5 mafeesh akbar meiny haaaaaaaaaaaaa");	
		iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for > operator");

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "name";
		arrSqlTerms[0]._strOperator = ">=";
		arrSqlTerms[0]._objValue = new String("ana string akbar w el ably da wala 7aga aaaaaaaaaaaaaaaaaaaaaaaa");		
		iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for >= operator");

		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "name";
		arrSqlTerms[0]._strOperator = "<";
		arrSqlTerms[0]._objValue = "lil";

		iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for < operator");
		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "name";
		arrSqlTerms[0]._strOperator = "<=";
		arrSqlTerms[0]._objValue = "pew";

		iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for <= operator");
	}
	
	@DisplayName("SelectFromTable_WithNothingMeetingConditionStringIndex_ShouldReturnEmptyIterator")
	@Test
	public void SelectWithUnMetConditionStringIndex() throws ClassNotFoundException, DBAppException, IOException 
	{
		database.createIndex("banadyMethod", "name", "nameIndex");
		
		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "name";
		arrSqlTerms[0]._strOperator = "!=";
		arrSqlTerms[0]._objValue = new String("nefsy 7ad yerood 3ala el dms bet3aty");
		Iterator<Tuple>iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for != operator");
		
		insertRandomTuples(20);
		
		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "name";
		arrSqlTerms[0]._strOperator = "=";
		arrSqlTerms[0]._objValue = new String("bet5onak");
		iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for = operator");
		
		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "name";
		arrSqlTerms[0]._strOperator = ">";
		arrSqlTerms[0]._objValue = new String("dana string kebeer fas5 mafeesh akbar meiny haaaaaaaaaaaaa");	
		iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for > operator");
		
		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "name";
		arrSqlTerms[0]._strOperator = ">=";
		arrSqlTerms[0]._objValue = new String("ana string akbar w el ably da wala 7aga aaaaaaaaaaaaaaaaaaaaaaaa");		
		iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for >= operator");
		
		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "name";
		arrSqlTerms[0]._strOperator = "<";
		arrSqlTerms[0]._objValue = "lil";
		
		iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for < operator");
		arrSqlTerms = new SQLTerm[1];
		arrSqlTerms[0] = new SQLTerm();
		arrSqlTerms[0]._strTableName = "banadyMethod";
		arrSqlTerms[0]._strColumnName = "name";
		arrSqlTerms[0]._strOperator = "<=";
		arrSqlTerms[0]._objValue = "pew";
		
		iteratorSelectionResult = database.selectFromTable(arrSqlTerms, new String[0]);
		assertTrue(!iteratorSelectionResult.hasNext(),"Found result when it should've been empty for <= operator");
	}
	// double select tests

}