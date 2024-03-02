
/** * @author Wael Abouelsaadat */ 

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


public class DBApp {

	ArrayList<Table> tables = new ArrayList<Table>();
	HelperMethods helper = new HelperMethods();

	public DBApp( ){
		
	}

	// this does whatever initialization you would like 
	// or leave it empty if there is no code you want to 
	// execute at application startup 
	public void init( ){
	}

	
	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data 
	// type as value
	public void createTable(String strTableName, 
							String strClusteringKeyColumn,  
							Hashtable<String,String> htblColNameType) throws DBAppException, IOException{
		//check if this table already exists
		if(tables.contains(strTableName))
			throw new DBAppException("A table of this name already exists.");
		
		Table currTable = new Table(strTableName);
		tables.add(currTable);
		// write onto the metadata file the following info: 
		// TableName,ColumnName, ColumnType, ClusteringKey, IndexName, IndexType
		helper.writeCSV(strTableName, strClusteringKeyColumn, htblColNameType);
	}


	// following method creates a B+tree index 
	public void createIndex(String   strTableName,
							String   strColName,
							String   strIndexName) throws DBAppException{
		
		throw new DBAppException("not implemented yet");
	}


	// following method inserts one row only. 
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, 
								Hashtable<String,Object>  htblColNameValue) throws DBAppException, IOException{
		// TODO insert into index
		Table omar=null;
		//find table name & get its object
		for(int i=0;i<tables.size();i++) {
			Table table=tables.get(i);
			if(table.getName().equals(strTableName)) {
				omar=table;
				break;
			}
				
		}
		if(omar.equals(null)) 
			throw new DBAppException("table doesnot exist");

		//check if primary key is null
		String primaryKeyColName = "";
		List<List<String>> colDataTypes = helper.getMetaData(omar.getName());
		for(int i = 0;i<colDataTypes.size();i++)
		{
			if(colDataTypes.get(i).get(3).equals("True"))
			{
				primaryKeyColName = colDataTypes.get(i).get(1);
			}
		}
		
		if(htblColNameValue.get(primaryKeyColName) == null || !htblColNameValue.contains(primaryKeyColName))
			throw new DBAppException("Primary key is null");
		
		
		//check if all datatypes are correct
		Set<String> setOfColNames = htblColNameValue.keySet();
		
		for(int i = 0;i<colDataTypes.size();i++)
		{
			String tmp=(colDataTypes.get(i).get(2)).split(".")[2];
			if(tmp.equals("String")) 
				if(! (htblColNameValue.get(colDataTypes.get(i).get(1)) instanceof String)) 
					throw new DBAppException("data type is not string"); 
			
			if(tmp.equals("Integer")) 
				if(! (htblColNameValue.get(colDataTypes.get(i).get(1)) instanceof Integer)) 
					throw new DBAppException("data type is not integer"); 
				
			if(tmp.equals("Double")) 
				if(! (htblColNameValue.get(colDataTypes.get(i).get(1)) instanceof Double)) 
					throw new DBAppException("data type is not double"); 
				
			
				
		}
		
		throw new DBAppException("not implemented yet");
	}


	// following method updates one row only
	// htblColNameValue holds the key and new value 
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, 
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue   )  throws DBAppException{
	
		throw new DBAppException("not implemented yet");
	}


	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search 
	// to identify which rows/tuples to delete. 	
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, 
								Hashtable<String,Object> htblColNameValue) throws DBAppException{
	
		throw new DBAppException("not implemented yet");
	}


	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, 
									String[]  strarrOperators) throws DBAppException{
										
		return null;
	}


	public static void main( String[] args ){
	
	try{
			String strTableName = "Student";
			DBApp	dbApp = new DBApp( );
			
			Hashtable htblColNameType = new Hashtable( );
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.double");
			dbApp.createTable( strTableName, "id", htblColNameType );
//			dbApp.createIndex( strTableName, "gpa", "gpaIndex" );
//
//			Hashtable htblColNameValue = new Hashtable( );
//			htblColNameValue.put("id", new Integer( 2343432 ));
//			htblColNameValue.put("name", new String("Ahmed Noor" ) );
//			htblColNameValue.put("gpa", new Double( 0.95 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", new Integer( 453455 ));
//			htblColNameValue.put("name", new String("Ahmed Noor" ) );
//			htblColNameValue.put("gpa", new Double( 0.95 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", new Integer( 5674567 ));
//			htblColNameValue.put("name", new String("Dalia Noor" ) );
//			htblColNameValue.put("gpa", new Double( 1.25 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", new Integer( 23498 ));
//			htblColNameValue.put("name", new String("John Noor" ) );
//			htblColNameValue.put("gpa", new Double( 1.5 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", new Integer( 78452 ));
//			htblColNameValue.put("name", new String("Zaky Noor" ) );
//			htblColNameValue.put("gpa", new Double( 0.88 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//
//			SQLTerm[] arrSQLTerms;
//			arrSQLTerms = new SQLTerm[2];
//			arrSQLTerms[0]._strTableName =  "Student";
//			arrSQLTerms[0]._strColumnName=  "name";
//			arrSQLTerms[0]._strOperator  =  "=";
//			arrSQLTerms[0]._objValue     =  "John Noor";
//
//			arrSQLTerms[1]._strTableName =  "Student";
//			arrSQLTerms[1]._strColumnName=  "gpa";
//			arrSQLTerms[1]._strOperator  =  "=";
//			arrSQLTerms[1]._objValue     =  new Double( 1.5 );
//
//			String[]strarrOperators = new String[1];
//			strarrOperators[0] = "OR";
//			// select * from Student where name = "John Noor" or gpa = 1.5;
//			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
		}
		catch(Exception exp){
			exp.printStackTrace( );
		}
	}

}