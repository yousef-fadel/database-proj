/** * @author Wael Abouelsaadat */
package com.goat.database;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Vector;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

@SuppressWarnings("deprecation")
public class DBApp {

	Vector<Table> tables=new Vector<Table>();

	public DBApp() throws ClassNotFoundException, DBAppException {
		init();
		if(tables==null)
			System.out.println("Was not able to intialize the tables for some reason; pray");


	}

	// this does whatever initialization you would like
	// or leave it empty if there is no code you want to
	// execute at application startup
	public void init() throws ClassNotFoundException, DBAppException {
		try 
		{
			new File("./resources/metadata.csv").createNewFile();
		} 
		catch (IOException e) {
			System.out.println("An error occured while attempting to create a metadata.csv file");
			e.printStackTrace();
		}
		new File("./tables").mkdirs();
		tables = new Vector<Table>();
		File file = new File("./tables");
		String[] names = file.list();

		for(String name : names)
		{
			if (new File("./tables/" + name).isDirectory())
			{
				tables.add((Table) deserializeData("./tables/"+name+ "/info.ser"));
			}
		}
		File configFilePath = new File("resources/DBApp.config");
		if(!configFilePath.exists())
			throw new DBAppException("The config file was not found");
	}

	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data
	// type as value
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException, ClassNotFoundException {
		checkCreateTable(strTableName,strClusteringKeyColumn,htblColNameType);

		// write onto the metadata file the following info:
		// TableName,ColumnName, ColumnType, ClusteringKey, IndexName, IndexType
		writeMetadata(strTableName, strClusteringKeyColumn, htblColNameType);

		// create a directory to store pages of this table for later 
		// + create a file called info.ser that stores all info about this table
		String tableFilePath = "./tables/" + strTableName + "/";
		File filepath = new File(tableFilePath + "/indices/");
		filepath.mkdirs();

		Table currTable = new Table(strTableName,tableFilePath);
		tables.add(currTable);
		currTable = currTable.serializeAndDeleteTable();

		System.out.println("Successfully created table " + strTableName);
	}

	// following method creates a B+tree index
	public void createIndex(String strTableName, String strColName, String strIndexName) throws DBAppException, IOException, ClassNotFoundException 
	{
		checkCreateIndex(strTableName, strColName, strIndexName);

		Table omar = getTable(strTableName);
		// update the metadata file and change the column's index type to b+Tree
		updateMetadataIndex(strTableName,strColName,strIndexName);
		// create the index and store all values in the table inside of the index
		Index index = new Index(strIndexName, strColName, omar.filepath);
		omar.insertRowsIntoIndex(strColName,index);
		omar.indexNames.add(strIndexName);
		index = null;
		omar = omar.serializeAndDeleteTable();
		System.out.println("Successfully created B+tree for " + strColName);
	}

	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException, ClassNotFoundException {

		checkInsert(strTableName,htblColNameValue);
		Table omar = getTable(strTableName);

		// get the value of the pk so we can create the object tuple
		List<List<String>> tableInfo = getColumnData(omar.name);
		String primaryKeyColName = getPrimaryKeyName(tableInfo);
		Tuple tuple = new Tuple(htblColNameValue.get(primaryKeyColName), htblColNameValue);

		// i pass the column name to check if this column has an index later on
		omar.insertTupleIntoTable(tuple, primaryKeyColName);
		omar = omar.serializeAndDeleteTable();
		System.out.println(" " + tuple.toString() + " into " + strTableName);
	}

	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.	
	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException 
	{
		checkUpdate(strTableName,strClusteringKeyValue,htblColNameValue);

		Table omar = getTable(strTableName);
		// turn the string clustering key value into a generic object we can use

		List<List<String>> tableInfo = getColumnData(omar.name);
		String primaryKeyColName = getPrimaryKeyName(tableInfo);
		Object clusteringKeyValue = loadDataTypeOfClusteringKey(strClusteringKeyValue,omar);
		omar.updateTuple(clusteringKeyValue,htblColNameValue,primaryKeyColName);
		omar = omar.serializeAndDeleteTable();

	}

	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, ClassNotFoundException, IOException {
		checkDelete(strTableName,htblColNameValue);
		Table basyo = getTable(strTableName);
		basyo.deleteFromTable(htblColNameValue);
	}

	@SuppressWarnings("rawtypes")
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException, IOException, ClassNotFoundException 
	{
		checkSelect(arrSQLTerms, strarrOperators);
		String table_Name=arrSQLTerms[0]._strTableName;
		Table basyo = getTable(table_Name);
		if(basyo==null)
			throw new DBAppException("Table name does not exist");

		return (basyo.selectTable(arrSQLTerms,strarrOperators));
	}

	public Iterator parseSQL(StringBuffer strBufferSQL) throws DBAppException, ClassNotFoundException
	{
		MySQListener listener = new MySQListener();
		Iterator result = listener.parse(strBufferSQL);
		return result;
	}
	// ------------------------------------------CHECK-----------------------------------------------------

	public void checkCreateTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {
		// check if this table already exists
		for (int i = 0; i < tables.size(); i++)
			if (tables.get(i).name.equals(strTableName)) 
				throw new DBAppException("A table of this name already exists");

		if(strTableName == null || strClusteringKeyColumn == null || htblColNameType == null || htblColNameType.isEmpty() ||
				strTableName.isBlank() || strTableName.isEmpty())
			throw new DBAppException("One of the inputs is null or empty");
		String [] possibleDataTypes = {"java.lang.Integer","java.lang.String","java.lang.Double"};
		Iterator<Map.Entry <String,String>> colData = htblColNameType.entrySet().iterator();
		// the reason for two loops is because we want to check the data before starting to write onto
		// the csv file; we cant do both at the same time
		boolean clusteringKeyExists = false;
		while(colData.hasNext())
		{
			Map.Entry<String,String> currCol = colData.next();
			String colName = currCol.getKey();
			String colDataType = currCol.getValue();
			if(!Arrays.stream(possibleDataTypes).anyMatch(colDataType::equals))
				throw new DBAppException("Column has invalid datatype");
			if(colName.equals(strClusteringKeyColumn))
				clusteringKeyExists = true;
		}
		if(!clusteringKeyExists)
			throw new DBAppException("Clustering key does not exist in columns");
	}

	public void checkCreateIndex(String strTableName, String strColName, String strIndexName) throws DBAppException, IOException {
		Table omar = getTable(strTableName);
		if (omar == null)
			throw new DBAppException("Table does not exist");

		if(strColName==null || strIndexName==null || strIndexName.isBlank() || strIndexName.isEmpty() || strColName.isBlank() )
			throw new DBAppException("One of the inputs was null or empty");

		// check that there is no index for this column specifcally
		List<List<String>> tableInfo = getColumnData(omar.name);
		for (int i = 0; i < tableInfo.size(); i++)
			if(tableInfo.get(i).get(1).equals(strColName) && !tableInfo.get(i).get(5).equals("null"))
				throw new DBAppException("Index already exists");

		//check that this name is unique (ie no other index already has this name)
		for(String indexName:omar.indexNames)
			if(indexName.equals(strIndexName))
				throw new DBAppException("A similar index name already exists for " + omar.name);

		// check if this column exists aslan
		ArrayList<String> colTableNames = getColumnNames(tableInfo);
		for(int i = 0;i<colTableNames.size();i++)
		{
			if(colTableNames.contains(strColName))
				break;
			if(i==colTableNames.size()-1)
				throw new DBAppException("Column name not found");
		}

	}

	public void checkInsert(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {
		// check if the table exists
		Table omar = getTable(strTableName);
		if (omar == null)
			throw new DBAppException("Table does not exist");


		// get the names of the columns in the table
		List<List<String>> tableInfo = getColumnData(omar.name);
		ArrayList<String> colTableNames = getColumnNames(tableInfo);

		if(htblColNameValue==null)
			throw new DBAppException("One of the inputs is null");
		// check that all columns in the table have a value in the hashtable
		for(String currCol:colTableNames)
			if (htblColNameValue.get(currCol) == null)
				throw new DBAppException("The hashtable is missing data for one of the columns");

		// check that the columns in the hashtable exist in the table
		Iterator<Map.Entry <String,Object>> colNameValueIterator = htblColNameValue.entrySet().iterator();
		while(colNameValueIterator.hasNext())
		{
			Map.Entry<String,Object> currCol = colNameValueIterator.next();
			String colName = currCol.getKey();
			if(!colTableNames.contains(colName))
				throw new DBAppException("The hashtable has an extra column that does not exist in the table");
		}

		// check if all datatypes are correct
		for (int i = 0; i < tableInfo.size(); i++) {
			String colType = tableInfo.get(i).get(2);
			String colName = tableInfo.get(i).get(1);	
			if (colType.equals("java.lang.String")) {
				if (!(htblColNameValue.get(colName) instanceof String))
					throw new DBAppException("A column was inserted with the wrong datatype");
			}
			else if (colType.equals("java.lang.Integer")) {
				if (!(htblColNameValue.get(colName) instanceof Integer))
					throw new DBAppException("A column was inserted with the wrong datatype");
			}
			else if (colType.equals("java.lang.Double")) {
				if (!(htblColNameValue.get(colName) instanceof Double))
					throw new DBAppException("A column was inserted with the wrong datatype");
			}
			else // this exception should not be thrown at all; if it has then something has gone wrong in createTable()
				throw new DBAppException("The created table has an error in it's datatypes; please delete the table and try again");
		}

		String primaryKeyColName = getPrimaryKeyName(tableInfo);
		if(primaryKeyColName == null)
			throw new DBAppException("An error occured while looking for primary key; please try again");
	}

	public void checkUpdate(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException
	{
		Table omar = getTable(strTableName);
		if (omar == null)
			throw new DBAppException("Table does not exist");

		if(htblColNameValue==null || htblColNameValue.isEmpty() || strClusteringKeyValue == null 
				|| strClusteringKeyValue.isBlank() || strClusteringKeyValue.isEmpty())
			throw new DBAppException("One of the inputs was null or empty");

		// all strings in the hashtable are columns in the table
		List<List<String>> tableInfo = getColumnData(omar.name);

		String primaryKeyColumn = "";
		// get the primary key column name for later use
		for(int i = 0;i<tableInfo.size();i++)
		{
			if(tableInfo.get(i).get(3).equals("True"))
			{
				primaryKeyColumn = tableInfo.get(i).get(1);
				break;
			}
		}

		ArrayList<String> colTableNames = getColumnNames(tableInfo);
		Iterator<Map.Entry <String,Object>> colNameValueIterator = htblColNameValue.entrySet().iterator();
		while(colNameValueIterator.hasNext())
		{
			Map.Entry<String,Object> currCol = colNameValueIterator.next();
			String colName = currCol.getKey();
			if(!colTableNames.contains(colName))
				throw new DBAppException("The hashtable has an extra column that does not exist in the table");
			if(colName.equals(primaryKeyColumn))
				throw new DBAppException("The hashtable contains the primary key, which cannot be updated");
		}

		// all datatypes in the hashtable correct (ex: attempting to update a integer column with a string)
		for(Map.Entry<String, Object> updateEntry : htblColNameValue.entrySet())
		{
			for (int i = 0; i < tableInfo.size(); i++) 
			{
				String colName = tableInfo.get(i).get(1);
				String colType = tableInfo.get(i).get(2);
				if(colName.equals(updateEntry.getKey()))
					if(!updateEntry.getValue().getClass().getCanonicalName().equals(colType))
						throw new DBAppException("Unexpected datatype for one of the updated columns");
			}
		}




	}

	public void checkDelete(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {
		// check if the table exists
		Table omar = getTable(strTableName);
		if (omar == null)
			throw new DBAppException("Table does not exist");

		if(htblColNameValue == null)
			throw new DBAppException("One of the inputs was null");
		// get the names of the columns in the table
		List<List<String>> tableInfo = getColumnData(omar.name);
		ArrayList<String> colTableNames = getColumnNames(tableInfo);

		// check that the columns in the hashtable exist in the table
		Iterator<Map.Entry <String,Object>> colNameValueIterator = htblColNameValue.entrySet().iterator();
		while(colNameValueIterator.hasNext())
		{
			Map.Entry<String,Object> currCol = colNameValueIterator.next();
			String colName = currCol.getKey();
			if(!colTableNames.contains(colName))
				throw new DBAppException("The hashtable has an extra column that does not exist in the table");
		}

		// all datatypes in the hashtable correct (ex: attempting to delete a integer column with a string)
		for(Map.Entry<String, Object> updateEntry : htblColNameValue.entrySet())
		{
			for (int i = 0; i < tableInfo.size(); i++) 
			{
				String colName = tableInfo.get(i).get(1);
				String colType = tableInfo.get(i).get(2);
				if(colName.equals(updateEntry.getKey()))
					if(!updateEntry.getValue().getClass().getCanonicalName().equals(colType))
						throw new DBAppException("Unexpected datatype for one of the deletion conditions");
			}
		}

	}

	public void checkSelect(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		for(int i=0;i<arrSQLTerms.length;i++) {
			if(arrSQLTerms[i]==null) 
				throw new DBAppException("Statement array has null values");
		}

		for(int i=0;i<strarrOperators.length;i++) {
			if(strarrOperators[i]==null) 
				throw new DBAppException("Operator array has null values");
		}
		if(!(arrSQLTerms.length-1==strarrOperators.length)) 
			throw new DBAppException("Number of operators incorrect");

	}

	// ------------------------------------------HELPER--------------------------------------------------------

	public boolean searchTableNoIndex(Hashtable<String, Object> htblColNameValue,Table omar) throws IOException, ClassNotFoundException {
		ArrayList<Tuple> tableTuples=new ArrayList<Tuple>();
		List<List<String>> tableInfo = getColumnData(omar.name);
		String primaryKeyColName = getPrimaryKeyName(tableInfo);
		Datatype primvalue=new Datatype(htblColNameValue.get(primaryKeyColName));

		for(String pageName:omar.pageNames) {
			Page page = (Page) DBApp.deserializeData(omar.filepath + pageName);
			tableTuples.addAll(page.tuples);	
		}
		int left = 0;
		int right = tableTuples.size() - 1;

		while (left <= right) {
			int mid = left + (right - left) / 2;
			Tuple midElement = tableTuples.get(mid);
			Datatype tupleprimary=new Datatype(midElement.Primary_key);
			int comparison = tupleprimary.compareTo(primvalue);

			if (comparison == 0) {
				return true; // Target found
			} else if (comparison < 0) {
				left = mid + 1; // Search in the right half
			} else {
				right = mid - 1; // Search in the left half
			}
		}

		return false;

	}

	public boolean searchTableIndex(Hashtable<String, Object> htblColNameValue,Table omar) throws IOException, ClassNotFoundException 
	{
		List<List<String>> tableInfo = getColumnData(omar.name);
		String primaryKeyColName = getPrimaryKeyName(tableInfo);
		Datatype primvalue=new Datatype(htblColNameValue.get(primaryKeyColName));
		Index index=omar.getIndexWithColName(primaryKeyColName);
		if(index.searchIndex(primvalue)==null)
			return false;
		else
			return true;


	}

	public static String getPrimaryKeyName(List<List<String>> tableInfo)
	{
		for (int i = 0; i < tableInfo.size(); i++)
			if (tableInfo.get(i).get(3).equals("True"))
				return tableInfo.get(i).get(1);

		return null;		
	}

	public static void serializedata(Object o, String filename) {
		try {
			FileOutputStream file;
			if(filename.contains(".ser"))
				file = new FileOutputStream(filename);
			else
				file = new FileOutputStream(filename + ".ser");
			ObjectOutputStream out = new ObjectOutputStream(file);
			out.writeObject(o);
			out.close();
			file.close();
		} catch (IOException e) {
			e.printStackTrace();
		}


	}

	public static Object deserializeData(String filename) throws ClassNotFoundException {
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

		} catch (IOException i) {
			System.out.println("Could not deserialize " + filename + " ,returning null");
			return null;
		}
	}

	public Table getTable(String tableName) {
		for (int i = 0; i < tables.size(); i++) {
			if (tables.get(i).name.equals(tableName)) {
				Table t = tables.get(i);
				return t;
			}

		}
		return null;
	}

	private Object loadDataTypeOfClusteringKey(String strKeyValue, Table table) throws IOException, DBAppException
	{
		List<List<String>> tableInfo = getColumnData(table.name);
		for(int i = 0 ;i<tableInfo.size();i++)
			if(tableInfo.get(i).get(3).equals("True"))
			{
				try {
					String strColType = tableInfo.get(i).get(2);
					Class<?> classType = Class.forName(strColType);
					Constructor<?> constructor = classType.getConstructor(String.class);
					return constructor.newInstance(strKeyValue);

				} catch (ClassNotFoundException | NoSuchMethodException | InstantiationException |
						IllegalAccessException | InvocationTargetException e) {
					throw new DBAppException("There was a problem when parsing the clustering key");
				}
			}
		return null;

	}
	// given a table name, primary key, and information about the table columns, it writes onto the csv file 
	// all info about this table; also checks two exceptions: if the clustering key exists in the hashtable,
	// and if the datatype of all columns in the hashtable are valid
	public void writeMetadata(String strTableName, String strClusteringKeyColumn,Hashtable<String,String> htblColNameType) throws IOException, DBAppException
	{
		File file = new File("./resources/metadata.csv"); 
		Iterator<Map.Entry <String,String>> colData = htblColNameType.entrySet().iterator();



		FileWriter outputfile = new FileWriter(file,true); 
		CSVWriter writer = new CSVWriter(outputfile);
		colData = htblColNameType.entrySet().iterator();	  
		while(colData.hasNext())
		{
			Map.Entry<String,String> currCol = colData.next();
			String colName = currCol.getKey();
			String colDataType = currCol.getValue();
			if(strClusteringKeyColumn.equals(colName))
			{
				String[] header = {strTableName, colName, colDataType, "True", "null", "null"};
				writer.writeNext(header);
			}
			else
			{
				String[] header = {strTableName, colName, colDataType, "False", "null", "null"};
				writer.writeNext(header);
			}
		}
		writer.close();


	}

	//given a table name, returns a 2D list containing all information about its columns
	public static List<List<String>> getColumnData(String tableName) throws IOException
	{
		List<List<String>> records = new ArrayList<List<String>>();
		try (CSVReader csvReader = new CSVReader(new FileReader("./resources/metadata.csv"));) {
			String[] values = null;
			while ((values = csvReader.readNext()) != null) {
				if(values[0].equals(tableName))
					records.add(Arrays.asList(values));
			}
		}		
		return records;
	}

	// given a 2d list containing the table info, it will return all the column names in that list
	// run getColumnData on the metafile before running this
	public static ArrayList<String> getColumnNames(List<List<String>> tableInfo)
	{
		ArrayList<String> colTableNames = new ArrayList<String>();
		for (int i = 0; i < tableInfo.size(); i++)
			colTableNames.add(tableInfo.get(i).get(1));
		return colTableNames;
	}

	private void updateMetadataIndex(String strTableName,String strColName, String strIndexName) throws IOException
	{
		CSVReader reader = new CSVReader(new FileReader("resources/metadata.csv"));
		List<String[]> metadata = reader.readAll();
		for (int i = 0; i < metadata.size(); i++)
			if(metadata.get(i)[0].equals(strTableName) && metadata.get(i)[1].equals(strColName))
			{
				metadata.get(i)[4] = strIndexName;
				metadata.get(i)[5] = "B+tree";
				reader.close();

				CSVWriter writer = new CSVWriter(new FileWriter("resources/metadata.csv"));
				writer.writeAll(metadata);
				writer.flush();
				writer.close();
				break;
			}

	}

	// ----------------------------------------------- MAIN -------------------------------------------------
	public static void main(String[] args) throws ClassNotFoundException, DBAppException, IOException
	{
		DBApp dbApp =new DBApp();	
	}


	// completely delete everything: meta file, tables, all pages
	private void format() throws IOException {
		Path dir = Paths.get("./tables"); 
		Files
		.walk(dir)
		.sorted(Comparator.reverseOrder())
		.forEach(path -> {
			try {Files.delete(path);} 
			catch (IOException e) {e.printStackTrace();}});
		new File("./resources/tables.ser").delete();
		new File("./resources/metadata.csv").delete();

	}

	private void test5() throws ClassNotFoundException, DBAppException, IOException
	{
		Random random = new Random();

		Hashtable<String,String> htbl = new Hashtable<String,String>();
		htbl.put("id", "java.lang.Integer");
		htbl.put("age", "java.lang.Integer");
		htbl.put("gpa", "java.lang.Double");
		htbl.put("name", "java.lang.String");
		createTable("Vagabond", "id", htbl);
		Hashtable<String,Object> colData = new Hashtable<String, Object>();

		this.createIndex("Vagabond", "age", "ageIndex");
		int uniqueID[] = random.ints(0,2000).distinct().limit(1000).toArray();
		int possibleAge[] = {18,19,20,21,22,23,24};
		double possibleGPA[] = {1.2,0.7,3.2,4,2,2.3,1.8};
		String possibleName[] = {"Yousef","Jana","Kiryu","Popola","Rana","Maryam","Farida","Emil",
				"Eve","5ayen","Zoma","Musashi","Peter","01111146949","Kojiro"};
		//		String possibleName[] = {"01-203","582-495","2985-2223","2-39"};
		for(int i=0;i<50;i++) {
			int age = possibleAge[random.nextInt(possibleAge.length)];
			int id = i;
			double gpa = possibleGPA[random.nextInt(possibleGPA.length)];
			String name = possibleName[random.nextInt(possibleName.length)];
			colData.put("id", new Integer(id));
			colData.put("age", new Integer(age));
			colData.put("gpa", new Double(gpa));
			colData.put("name", new String(name));
			this.insertIntoTable( "Vagabond" , colData );
		}

		File deleteFile = new File("./resources/test5output.txt");
		deleteFile.delete();
		PrintWriter out = new PrintWriter("./resources/test5output.txt");
		Table windy = getTable("Vagabond");
		for(String pageName:windy.pageNames)
		{
			Page currPage = (Page) deserializeData(windy.filepath + pageName);
			System.out.println(currPage);
			out.println(currPage);
		}
		out.close();
	}

	private void takeIinputFromConsole() throws ClassNotFoundException, DBAppException, IOException
	{
		DBApp dbApp = new DBApp();
		while(true)
		{
			Scanner sc = new Scanner(System.in);
			StringBuilder sqlQuery = new StringBuilder();
			String line;

			while (!(line = sc.nextLine()).isEmpty()) {
				sqlQuery.append(line).append("\n");
			}

			Iterator iterate = dbApp.parseSQL(new StringBuffer(sqlQuery.toString()));
			//		sc.close();
			dbApp.showTables();
			int count = 0;
			if(iterate!=null)
				while(iterate.hasNext())
				{
					System.out.println(iterate.next());
					count++;
				}
			System.out.println(count);
		}
	}

	private void showTables() throws ClassNotFoundException, DBAppException, IOException
	{
		DBApp dbApp = new DBApp();
		File deleteFile = new File("./resources/tables.txt");
		deleteFile.delete();
		PrintWriter out = new PrintWriter("./resources/tables.txt");
		for(Table table:dbApp.tables)
		{
			out.println(table.name);
			for(String pageName:table.pageNames)
			{
				Page currPage = (Page) deserializeData(table.filepath + pageName);
				//			System.out.println(currPage);
				out.println(currPage);
			}

			out.println("----------------------------------");

		}
		out.close();

	}



}