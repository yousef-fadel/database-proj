/** * @author Wael Abouelsaadat */
package com.goat.database;

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

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
		
		// write onto the metadata file the following info:
		// TableName,ColumnName, ColumnType, ClusteringKey, IndexName, IndexType
		// method also checks if the datatypes are valid and that the clustering key exists in the table
		writeCSV(strTableName, strClusteringKeyColumn, htblColNameType);
		
		// create a directory to store pages of this table for later 
		// + create a file called info.ser that stores all info about this table
		File filepath = new File("./tables/" + strTableName + "/indices/");
		filepath.mkdirs();

		Table currTable = new Table(strTableName, "./tables/" + strTableName + "/");
		tables.add(currTable);
		serializedata(currTable, "./tables/" + currTable.name + "/info");


	}

	// TODO take fanout from config file
	// following method creates a B+tree index
	public void createIndex(String strTableName, String strColName, String strIndexName) throws DBAppException, IOException, ClassNotFoundException {

		Table omar = getTable(strTableName);
		if (omar == null)
			throw new DBAppException("Table does not exist");
		
		List<List<String>> tableInfo = getColumnData(omar.name);
		ArrayList<String> colTableNames = new ArrayList<String>();
		for (int i = 0; i < tableInfo.size(); i++)
			if(tableInfo.get(i).get(1).equals(strColName) && !(tableInfo.get(i).get(5).equals(null)))
				throw new DBAppException("Index already exists");
				
		for(int i = 0;i<colTableNames.size();i++)
		{
			if(colTableNames.contains(strColName))
				break;
			if(i==colTableNames.size()-1)
				throw new DBAppException("Column name not found");
		}
		
		File metadataLocation = new File("resources/metadata.csv");
		// Read existing file 
		CSVReader reader = new CSVReader(new FileReader(metadataLocation), ',');
		List<String[]> metadata = reader.readAll();
		for (int i = 0; i < metadata.size(); i++)
			if(metadata.get(i)[0].equals(strTableName) && metadata.get(i)[1].equals(strColName))
			{
				// get CSV row column  and replace with by using row and column
				metadata.get(i)[4] = strIndexName;
				metadata.get(i)[5]= "B+tree";
				reader.close();

				CSVWriter writer = new CSVWriter(new FileWriter(metadataLocation), ',');
				writer.writeAll(metadata);
				writer.flush();
				writer.close();
				break;
			}
		Index index = new Index(strIndexName, omar.filepath);
		index.serializeIndex();
		
		omar.insertRowsIntoIndex(strColName,index);
		index = null;
		omar = omar.serializeAndDeleteTable();
		
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


		// get the names of the columns in the table
		List<List<String>> tableInfo = getColumnData(omar.name);
		ArrayList<String> colTableNames = new ArrayList<String>();
		for (int i = 0; i < tableInfo.size(); i++)
			colTableNames.add(tableInfo.get(i).get(1));

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
			String colType = (tableInfo.get(i).get(2));
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
		Tuple tuple = new Tuple(htblColNameValue.get(primaryKeyColName), htblColNameValue);
		omar.insertTupleIntoTable(tuple);
		omar = omar.serializeAndDeleteTable();
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

	private String getPrimaryKeyName(List<List<String>> tableInfo)
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

	// given a table name, primary key, and information about the table columns, it writes onto the csv file 
	// all info about this table; also checks two exceptions: if the clustering key exists in the hashtable,
	// and if the datatype of all columns in the hashtable are valid
	public void writeCSV(String strTableName, String strClusteringKeyColumn,Hashtable<String,String> htblColNameType) throws IOException, DBAppException
	{
		File file = new File("./resources/metadata.csv"); 
	    try { 
	        FileWriter outputfile = new FileWriter(file,true); 
	        CSVWriter writer = new CSVWriter(outputfile);
	        
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
	        
	        colData = htblColNameType.entrySet().iterator();	  
	        while(colData.hasNext())
	        {
	        	Map.Entry<String,String> currCol = colData.next();
	        	String colName = currCol.getKey();
	        	String colDataType = currCol.getValue();
	        	// check if it is valid data type aslan
	        	if(!Arrays.stream(possibleDataTypes).anyMatch(colDataType::equals))
	        		throw new DBAppException("Column has invalid datatype");
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
//		dbApp.format();
//		dbApp.test4();
//		dbApp.test3();
//		dbApp.test1(dbApp);
//		dbApp.test2(dbApp);
//		Table table = dbApp.getTable("table");
//		System.out.println(table.pageNames);
//		for(int i = 0;i<table.pageNames.size();i++)
//		{
//			Page page = (Page) deserializeData(table.filepath + table.pageNames.get(i));
//			System.out.println(page.tuples);
//		}


//		String strTableName = "Student";
//		DBApp	dbApp = new DBApp( );
		
		dbApp.createIndex( "table", "age", "kamal");
		Index index = (Index) deserializeData("./tables/table/indices/kamal.ser");
		System.out.println(index.searchIndex(new Datatype(18)));
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
		//			 this will delete all rows with gpa 0.75 and name Ahmed Noor
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
		htblColNameValue.put("id", new Integer( 5 ));
		dbApp.insertIntoTable( strTableName , htblColNameValue );

		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 7 ));
		dbApp.insertIntoTable( strTableName , htblColNameValue );

		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 20 ));
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
		dbApp.insertIntoTable( "table" , htblColNameValue );

	}

	private void test3() throws ClassNotFoundException, DBAppException, IOException
	{
		
		Hashtable<String,String> htbl = new Hashtable<String,String>();
		htbl.put("id", "java.lang.Integer");
		this.createTable("table", "id", htbl);
		Hashtable<String,Object> colData = new Hashtable<String, Object>();
		
		colData.put("id", new Integer(2));
		this.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 20 ));
		this.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 7 ));
		this.insertIntoTable( "table" , colData );
				
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 11 ));
		this.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 15 ));
		this.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 31 ));
		this.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 1 ));
		this.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 25 ));
		this.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 5 ));
		this.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 30 ));
		this.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 17 ));
		this.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 19 ));
		this.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 22 ));
		this.insertIntoTable( "table" , colData );
		
		Table table = this.getTable("table");
		for(int i = 0;i<table.pageNames.size();i++)
		{
			Page page = (Page) deserializeData(table.filepath + table.pageNames.get(i));
			System.out.println(page.tuples);
		}
		
	}
	private void test4() throws ClassNotFoundException, DBAppException, IOException
	{
		Hashtable<String,String> htbl = new Hashtable<String,String>();
		htbl.put("id", "java.lang.Integer");
		htbl.put("age", "java.lang.Integer");		
		this.createTable("table", "id", htbl);
		Hashtable<String,Object> colData = new Hashtable<String, Object>();
		
		colData.put("id", new Integer(2));
		colData.put("age", 18);
		this.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 20 ));
		colData.put("age", new Integer( 16 ));
		this.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 7 ));
		colData.put("age", new Integer( 16 ));
		this.insertIntoTable( "table" , colData );
				
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 11 ));
		colData.put("age", new Integer( 18 ));
		this.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 15 ));
		colData.put("age", new Integer( 16 ));
		this.insertIntoTable( "table" , colData );
		
		colData = new Hashtable<String, Object>();		
		colData.put("id", new Integer( 31 ));
		colData.put("age", new Integer( 11 ));
		this.insertIntoTable( "table" , colData );
		
		Table table = this.getTable("table");
		for(int i = 0;i<table.pageNames.size();i++)
		{
			Page page = (Page) deserializeData(table.filepath + table.pageNames.get(i));
			System.out.println(page);
		}
				
	}
}