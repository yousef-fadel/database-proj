/** * @author Wael Abouelsaadat */
package com.goat.database;

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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.io.FileUtils;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

public class DBApp {

	Vector<Table> tables;

	public DBApp() throws ClassNotFoundException {
		init();
		if(tables==null)
			System.out.println("Was not able to intialize the tables for some reason; pray");
	}

	// this does whatever initialization you would like
	// or leave it empty if there is no code you want to
	// execute at application startup
	@SuppressWarnings("unchecked")
	public void init() throws ClassNotFoundException {
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
//		tables = (Vector<Table>) deserializeData("./resources/tables.ser");

		//TODO check for config file
	}

	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data
	// type as value
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException, ClassNotFoundException {
		
		// check if this table already exists
		for (int i = 0; i < tables.size(); i++)
			if (tables.get(i).name.equals(strTableName))
				throw new DBAppException("A table of this name already exists");

		// create a directory to store pages of this table for later 
		// + create a file called info.ser that stores all info about this table
		File filepath = new File("./tables/" + strTableName);
		filepath.mkdirs();
		Table currTable = new Table(strTableName, filepath.getPath() + "/");
		tables.add(currTable);
		serializedata(currTable, "./tables/" + currTable.name + "/info");

		// write onto the metadata file the following info:
		// TableName,ColumnName, ColumnType, ClusteringKey, IndexName, IndexType
		writeCSV(strTableName, strClusteringKeyColumn, htblColNameType);
	}

	// following method creates a B+tree index
	public void createIndex(String strTableName, String strColName, String strIndexName) throws DBAppException {

		throw new DBAppException("not implemented yet");
	}

	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException, ClassNotFoundException {
		// TODO insert into index
		
		// check if the table exists
		Table omar = getTable(strTableName);
		if (omar == null)
			throw new DBAppException("Table does not exist");


		// check if primary key is null
		String primaryKeyColName = "";
		List<List<String>> colDataTypes = getColumnData(omar.name);
		// get primary key column name
		for (int i = 0; i < colDataTypes.size(); i++)
			if (colDataTypes.get(i).get(3).equals("True"))
				primaryKeyColName = colDataTypes.get(i).get(1);

		// if primary key is not part of the insertion
		// TODO change it so that if any column is null, throw an exception
		if (htblColNameValue.get(primaryKeyColName) == null)
			throw new DBAppException("Primary key was not found");

		// check if the columns inserted exist
		Set<String> setOfColNames = htblColNameValue.keySet();
		Iterator <String> iter = setOfColNames.iterator();
		while (iter.hasNext()) {
			String tmp = iter.next();
			for (int j = 0; j < colDataTypes.size(); j++) {
				if (colDataTypes.get(j).get(1).equals(tmp))
					break;
				if (j == colDataTypes.size() - 1)
					throw new DBAppException("Column inserted does not exist");

			}
		}

		// check if all datatypes are correct
		// TODO check if it is a valid datatype aslan (if you insert float for example, it will get accepted bardo)
		for (int i = 0; i < colDataTypes.size(); i++) {
			String tmp = (colDataTypes.get(i).get(2));
			if (tmp.equals("java.lang.String"))
				if (!(htblColNameValue.get(colDataTypes.get(i).get(1)) instanceof String))
					throw new DBAppException("A column was inserted with the wrong datatype");

			if (tmp.equals("java.lang.Integer"))
				if (!(htblColNameValue.get(colDataTypes.get(i).get(1)) instanceof Integer))
					throw new DBAppException("A column was inserted with the wrong datatype");

			if (tmp.equals("java.lang.double"))
				if (!(htblColNameValue.get(colDataTypes.get(i).get(1)) instanceof Double))
					throw new DBAppException("A column was inserted with the wrong datatype");
		}
		
		Tuple tuple = new Tuple(htblColNameValue.get(primaryKeyColName), htblColNameValue);
		omar.insertTupleIntoTable(tuple);
		omar = null;
//		System.out.println("Inserted " + tuple +" succesfully!");
	}

	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {

		throw new DBAppException("not implemented yet");
	}

	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

		throw new DBAppException("not implemented yet");
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {

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

	// given a table name, primary key, and information about the table columns, it writes onto the csv file 
	// all info about this table
	public void writeCSV(String strTableName, String strClusteringKeyColumn,Hashtable<String,String> htblColNameType) throws IOException
	{
		File file = new File("./resources/metadata.csv"); 
	    try { 
	        FileWriter outputfile = new FileWriter("./resources/metadata.csv",true); 
	        CSVWriter writer = new CSVWriter(outputfile);
	        Set<String> setOfKeys = htblColNameType.keySet();
	        for(String keys : setOfKeys)
	        {
	        	if(strClusteringKeyColumn.equals((String)keys))
	        	{
	        		String[] header = {strTableName, keys, htblColNameType.get(keys), "True", "null", "null"};
	        		writer.writeNext(header);
	        	}
	        	else
	        	{
	        		String[] header = {strTableName, keys, htblColNameType.get(keys), "False", "null", "null"};
	        		writer.writeNext(header);
	        	}

	        }
	 	    writer.close(); 
	    } 
	    catch (IOException e) { 
	        e.printStackTrace(); 
	    }	
	}
	
	//given a table name, returns a 2D list containing all information about its columns
	public List<List<String>> getColumnData(String tableName) throws IOException
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
	
	
	@SuppressWarnings({ "removal", "unchecked" })
	public static void main( String[] args ) throws ClassNotFoundException, DBAppException, IOException{
		DBApp dbApp =new DBApp();
		dbApp.format();
		dbApp.test3();
//		dbApp.test1(dbApp);
//		dbApp.test2(dbApp);

//
//		
//		
//
//		Page page = (Page) DBApp.deserializeData("./tables/Student/Student0.ser");
//		System.out.println(page.tuples);
//		page =  (Page) DBApp.deserializeData("./tables/Student/Student1.ser");
//		System.out.println(page.tuples);

////
//
//
//
//		//			 this will delete 1 row, the one with id 78452
//		htblColNameValue.clear( );
//		htblColNameValue.put("id", new Integer( 78452 ));
//		dbApp.deleteFromTable( "Student", htblColNameValue );
//
//		//			 this will delete all rows with gpa 0.75
//		htblColNameValue.clear( );
//		htblColNameValue.put("gpa", new Double( 0.75 ) );
//		dbApp.deleteFromTable( "Student", htblColNameValue );
//
//		//			 this will delete all rows with gpa 0.75 and name Ahmed Noor
//		htblColNameValue enteries are ANDED together in delete
//		htblColNameValue.clear( );
//		htblColNameValue.put("name", new String("Ahmed Noor" ) );  
//		htblColNameValue.put("gpa", new Double( 0.75 ) );
//		dbApp.deleteFromTable( "Student", htblColNameValue );
//
//
//		SQLTerm[] arrSQLTerms;
//		arrSQLTerms = new SQLTerm[2];
//		arrSQLTerms[0]._strTableName =  "Student";
//		arrSQLTerms[0]._strColumnName=  "name";
//		arrSQLTerms[0]._strOperator  =  "=";
//		arrSQLTerms[0]._objValue     =  "John Noor";
//
//		arrSQLTerms[1]._strTableName =  "Student";
//		arrSQLTerms[1]._strColumnName=  "gpa";
//		arrSQLTerms[1]._strOperator  =  "=";
//		arrSQLTerms[1]._objValue     =  new Double( 1.5 );
//
//		String[]strarrOperators = new String[1];
//		strarrOperators[0] = "OR";
//		// select * from Student where name = "John Noor" or gpa = 1.5;
//		Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);


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

	private void test1(DBApp	dbApp) throws ClassNotFoundException, DBAppException, IOException
	{

		String strTableName = "Student";
	
		
		Hashtable htblColNameType = new Hashtable( );
		htblColNameType.put("id", "java.lang.Integer");
		dbApp.createTable( strTableName, "id", htblColNameType );

		Hashtable htblColNameValue = new Hashtable( );
		htblColNameValue.put("id", new Integer( 21 ));
		dbApp.insertIntoTable( strTableName , htblColNameValue );

		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 5 ));
		dbApp.insertIntoTable( strTableName , htblColNameValue );

		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 3 ));
		dbApp.insertIntoTable( strTableName , htblColNameValue );

		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 8 ));
		dbApp.insertIntoTable( strTableName , htblColNameValue );
		
		

		
	}
	
	private void test2(DBApp	dbApp ) throws ClassNotFoundException, DBAppException, IOException
	{
		String strTableName = "Student";
		Hashtable htblColNameValue = new Hashtable( );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 25 ));
		dbApp.insertIntoTable( strTableName , htblColNameValue );

	}

	private void test3() throws ClassNotFoundException, DBAppException, IOException
	{
		DBApp database = new DBApp();
		
		Hashtable<String,String> htbl = new Hashtable<String,String>();
		htbl.put("id", "java.lang.Integer");
		database.createTable("table", "id", htbl);
		Hashtable<String,Object> colData = new Hashtable<String, Object>();
		
		colData.put("id", new Integer(2));
		database.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 20 ));
		database.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 7 ));
		database.insertIntoTable( "table" , colData );
				
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 11 ));
		database.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 15 ));
		database.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 31 ));
		database.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 1 ));
		database.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 25 ));
		database.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 5 ));
		database.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 30 ));
		database.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 17 ));
		database.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 19 ));
		database.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 22 ));
		database.insertIntoTable( "table" , colData );
		
		Table table = database.getTable("table");
		for(int i = 0;i<table.pageNames.size();i++)
		{
			Page page = (Page) deserializeData(table.filepath + table.pageNames.get(i));
			System.out.println(page.tuples);
		}
	}
}