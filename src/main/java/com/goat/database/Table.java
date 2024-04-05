package com.goat.database;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class Table implements java.io.Serializable{
	public Vector<String> pageNames; 
	public Vector<String> indexNames; // da 3ashan ana makasel afta7 el csv file mesh aktar
	public String filepath; //location all pages
	public String name; //table name
	int numberForPage; // this is needed as pageNames.size() will break once we delete a page


	/**
	 * @param name
	 * @param filepath Should be "tables/nameOfTable/"
	 */
	public Table(String name, String filepath)
	{
		this.name = name;
		this.filepath = filepath;
		this.pageNames = new Vector<String>();
		this.indexNames = new Vector<String>();
		numberForPage = 0;
	}


	public void insertTupleIntoTable(Tuple tuple) throws DBAppException, ClassNotFoundException
	{
		// check if this is the first tuple to be inserted in the table; if it is create a page and insert it
		if(pageNames.isEmpty())
		{
			Page firstPage = new Page(this.name + this.numberForPage , this.numberForPage++,this.filepath);
			pageNames.add(firstPage.name);
			firstPage.tuples.add(tuple);

			insertIntoIndex(tuple,firstPage.name);
			firstPage = firstPage.serializeAndDeletePage();
			this.serializeAndDeleteTable();

		}
		else
		{
			Page pageToInsertInto = findPageToInsert(tuple);
			insertIntoPage(tuple, pageToInsertInto);
			this.serializeTable();
		}

	}
	private Page findPageToInsert(Tuple tuple) throws ClassNotFoundException, DBAppException
	{
		int leftPage = 0;
		int rightPage = this.pageNames.size()-1;
		int middlePage;
		//binary search for the page; stop once 2 (or 1) pages are left
		while(rightPage-leftPage>1)
		{
			middlePage = (leftPage + rightPage)/2;
			Page currPage = (Page) DBApp.deserializeData(this.filepath  + this.pageNames.get(middlePage));
			Tuple currTuple = currPage.tuples.get(0); 
			if(tuple.compareTo(currTuple)<0)
				rightPage = middlePage - 1;
			else if(tuple.compareTo(currTuple)>0)
				leftPage = middlePage; // deeh momken te2leb infinte loop; ol yarab
			else
				throw new DBAppException("The primary key is a duplicate");
		}

		Page page1 = (Page) DBApp.deserializeData(this.filepath + (this.pageNames.get(leftPage)));
		Page page2 = (Page) DBApp.deserializeData(this.filepath + (this.pageNames.get(rightPage)));
		if(this.pageNames.size()==1)
		{
			page2 = null;
			return page1;
		}
		else
		{
			if(tuple.compareTo(page1.tuples.lastElement()) < 0)
			{
				page2 = null;
				return page1;
			}
			else
			{
				page1 = null;
				return page2;
			}

		}
	}
	private void insertIntoPage(Tuple tuple, Page page) throws DBAppException, ClassNotFoundException
	{
		// binary search for its location inside the page
		// gives us two (or one) possible locations we can insert into 
		int left = 0;
		int right = 0;
		if(!page.tuples.isEmpty())
			right = page.tuples.size()-1;
		int middle;
		while(right-left>1) 
		{
			middle = (right + left)/2;
			if(tuple.compareTo(page.tuples.get(middle))<0)
				right = middle - 1; // maybe remove this -1 idk anymore
			else if (tuple.compareTo(page.tuples.get(middle))>0)
				left = middle + 1;
			else 
				throw new DBAppException("The primary key is a duplicate");
		}

		// left and right come from binary search up above
		// check which position to insert into; if it is smaller than both values, then place it where the left is,
		// if it is bigger than left but smaller than right, insert in between
		// otherwise, insert at the end
		int start = -1;
		if(tuple.compareTo(page.tuples.get(left))==0 
				|| tuple.compareTo(page.tuples.get(right))==0)
			throw new DBAppException("The primary key is a duplicate");
		if(tuple.compareTo(page.tuples.get(left))<0)
			start = left;
		else if (tuple.compareTo(page.tuples.get(left))>0
				&& tuple.compareTo(page.tuples.get(right))>0)
			start = right+1;
		else
			start = right;

		page.tuples.insertElementAt(tuple, start);
		insertIntoIndex(tuple,page.name);
		// if after we insert our element an overflow occurs, we start shifting to next page
		if(page.tuples.size()>page.maxNoEnteries)
			shiftTuples(page);
		else
			page = page.serializeAndDeletePage();
	}

	private void shiftTuples(Page currPage) throws ClassNotFoundException, DBAppException
	{

		while(currPage.tuples.size() > currPage.maxNoEnteries)
		{ 
			// take the last element of the page, store it, and remove it from the end of the page
			Tuple tmp = currPage.tuples.lastElement();
			currPage.tuples.remove(currPage.tuples.size()-1);
			deleteFromIndex(tmp,currPage.name);
			// if we reach the end of our pages, create a new page and store the last element in it
			// otherwise, save our current page and move onto the next page and insert the tmp at the top
			if(this.pageNames.lastElement().equals(currPage.name))
			{
				Page newPage = new Page(this.name + (this.numberForPage), this.numberForPage++, this.filepath);
				this.pageNames.add(newPage.name);
				newPage.tuples.add(tmp);
				insertIntoIndex(tmp, newPage.name);
				newPage = newPage.serializeAndDeletePage();
				this.serializeTable();
				break;
			}
			currPage.serializePage();
			int nextPage = this.pageNames.indexOf(currPage.name) + 1;
			currPage = (Page) DBApp.deserializeData(this.filepath + this.pageNames.get(nextPage));
			currPage.tuples.insertElementAt(tmp, 0);	
			insertIntoIndex(tmp, currPage.name);
		}
		currPage = currPage.serializeAndDeletePage();


	}
	private void deleteFromIndex(Tuple tuple, String pageName) throws ClassNotFoundException
	{
		if(this.indexNames.isEmpty())
			return;

		for(String indexName : this.indexNames)
		{
			Index index = getIndexWithIndexName(indexName);
			Object tupleData = tuple.entry.get(index.columnName);
			index.deleteFromIndex(new Datatype(tupleData), pageName);
			index = index.serializeAndDeleteIndex();
		}

	}

	public void insertRowsIntoIndex(String strColName, Index index) throws ClassNotFoundException
	{
		for(int i =0;i<this.pageNames.size();i++)
		{
			Page currPage = (Page) DBApp.deserializeData(this.filepath + this.pageNames.get(i));
			for(Tuple tuple:currPage.tuples)
			{
				Object tupleValue = tuple.entry.get(strColName);
				Datatype dataTypeValue = new Datatype(tupleValue);
				index.insertIntoIndex(dataTypeValue, currPage.name);
			}
		}
		index = index.serializeAndDeleteIndex();

	}


	// checks the table for any existing index to insert into
	private void insertIntoIndex(Tuple tuple, String pageName) throws ClassNotFoundException
	{
		if(this.indexNames.isEmpty())
			return;

		for(String indexName : this.indexNames)
		{
			Index index = getIndexWithIndexName(indexName);
			Object tupleData = tuple.entry.get(index.columnName);
			index.insertIntoIndex(new Datatype(tupleData), pageName);
			index = index.serializeAndDeleteIndex();
		}
	}

	// ------------------------------------------------------------------UPDATE---------------------------------------------------
	public void updateTuple(Object clusteringKeyValue, Map.Entry<String,Object> updateValueEntry, String indexName) throws ClassNotFoundException, DBAppException, IOException
	{
		Page pageToUpdate = findPageToUpdate(clusteringKeyValue);
		updateTupleInPage(pageToUpdate,clusteringKeyValue,updateValueEntry,indexName);
	}

	private Page findPageToUpdate(Object clusteringKeyValue) throws ClassNotFoundException, DBAppException
	{
		int leftPage = 0;
		int rightPage = this.pageNames.size()-1;
		int middlePage;
		//binary search for the page; stop once 2 (or 1) pages are left
		while(rightPage-leftPage>1)
		{
			Datatype clusteringvalue =new Datatype(clusteringKeyValue);

			middlePage = (leftPage + rightPage)/2;
			Page currPage = (Page) DBApp.deserializeData(this.filepath  + this.pageNames.get(middlePage));
			Tuple currTuple = currPage.tuples.get(0); 
 			Datatype currTuplePrim=new Datatype(currTuple.Primary_key);

			if(clusteringvalue.compareTo(currTuplePrim)<0)
				rightPage = middlePage;
			else if(clusteringvalue.compareTo(currTuplePrim)>0)
				leftPage = middlePage; // deeh momken te2leb infinte loop; ol yarab
			else
				return (Page) DBApp.deserializeData(this.filepath + (this.pageNames.get(middlePage)));
		}

		Page page1 = (Page) DBApp.deserializeData(this.filepath + (this.pageNames.get(leftPage)));
		Page page2 = (Page) DBApp.deserializeData(this.filepath + (this.pageNames.get(rightPage)));
		
		Datatype clusteringvalue =new Datatype(clusteringKeyValue);
		Datatype lastElement=new Datatype(page1.tuples.lastElement().Primary_key);
		if(clusteringvalue.compareTo(lastElement) <= 0)
		{
			page2 = null;
			return page1;
		}
		else
		{
			page1 = null;
			return page2;
		}


	}

	private void updateTupleInPage(Page page,Object clusteringKeyValue, Map.Entry<String,Object> updateValueEntry, String indexName) throws IOException, ClassNotFoundException {

		Datatype clusteringKeyValueDatatype = new Datatype(clusteringKeyValue);
		int left = 0;
		int right =page.tuples.size()-1;
		Vector<Tuple> pageTuples =  page.tuples;
		while(left<=right)
		{
			int middle = (left+right)/2;
			Datatype middleTuple = new Datatype(pageTuples.get(middle).Primary_key);
			if(middleTuple.compareTo(clusteringKeyValueDatatype)==0)
			{
				if(!indexName.equals("null"))
				{
					Index indexedCol = getIndexWithIndexName(indexName);
					indexedCol.deleteFromIndex(new Datatype(pageTuples.get(middle).entry.get(updateValueEntry.getKey())), page.name);
					indexedCol.insertIntoIndex(new Datatype(updateValueEntry.getValue()), page.name);
					indexedCol = indexedCol.serializeAndDeleteIndex();
				}
				pageTuples.get(middle).entry.put(updateValueEntry.getKey(), updateValueEntry.getValue());
				page = page.serializeAndDeletePage();
				return;
			}
			else if(middleTuple.compareTo(clusteringKeyValueDatatype)>0)
				right = middle-1;
			else
				left = middle+1;

		}
		System.out.println("Did not find clustering key, aborting update");
	}


	// ----------------------------- DELETE --------------
	
	public void deleteFromTable(Hashtable<String, Object> htblColNameValue) throws ClassNotFoundException, IOException
	{
		ArrayList<String> resultSoFar = new ArrayList<String>();
		Iterator<Map.Entry <String,Object>> colNameValueIterator = htblColNameValue.entrySet().iterator();
		 while(colNameValueIterator.hasNext())
		 {
			 Map.Entry<String,Object> deletionCondition = colNameValueIterator.next();
			 ArrayList<String> deletionConditionResult;
			 Index colIndex = getIndexWithColName(deletionCondition.getKey()); 
			 if(colIndex==null)
				 deletionConditionResult = findTuplesSatsifyingDeletion(deletionCondition);
			 else
				 deletionConditionResult = findTuplesSatsifyingDeletionIndex(deletionCondition,colIndex);
			 if(resultSoFar.isEmpty())
				 resultSoFar = deletionConditionResult;
			 else
				 resultSoFar =intersect(resultSoFar,deletionConditionResult); 
		 }
		 
		 deleteTuples(resultSoFar);
	}
	
	private ArrayList<String> findTuplesSatsifyingDeletion(Map.Entry<String, Object> deletionCondition) throws ClassNotFoundException
	{
		ArrayList<String> result = new ArrayList<String>();
		for(String pageName:this.pageNames)
		{
			Page page = (Page) DBApp.deserializeData(this.filepath + pageName);
			for(int i = 0 ; i<page.tuples.size();i++)
				if(page.tuples.get(i).entry.get(deletionCondition.getKey()).equals(deletionCondition.getValue()))
					result.add( page.name + "-" + i); // pagenumber - tuple pos
		}
		return result;
	}
	private ArrayList<String> findTuplesSatsifyingDeletionIndex(Map.Entry<String, Object> deletionCondition,Index colIndex) throws ClassNotFoundException
	{
		ArrayList<String> result = new ArrayList<String>();
		// get page locations from index
		Vector<String> pageResult = colIndex.searchIndex(new Datatype(deletionCondition.getValue()));
		// sheel duplicates from pageResult
		LinkedHashSet<String> hashSet = new LinkedHashSet<String>(pageResult);  
		pageResult.clear(); 
		pageResult.addAll(hashSet); 
		// add tuple pos to each page
		for(String pageName:pageResult)
		{
			Page page = (Page) DBApp.deserializeData(this.filepath + pageName);
			for(int i = 0;i<page.tuples.size();i++)
			{
				Tuple tuple = page.tuples.get(i);
				if(tuple.entry.get(deletionCondition.getKey()).equals(deletionCondition.getValue()))			
					result.add(pageName + "-" + i); 
			}
		}
		return result;
	}
	private ArrayList<String> intersect(ArrayList<String> firstList, ArrayList<String> secondList)
	{
		ArrayList<String> result = new ArrayList<String>();
		for(String currentPosition:firstList)
			if(secondList.contains(currentPosition))
				result.add(currentPosition);
		return result;
		
	}
	
	private void deleteTuples(ArrayList<String> deletionTuples) throws ClassNotFoundException
	{
		for(int i = deletionTuples.size()-1 ;i>=0;i--)
		{
			String tuplePosition = deletionTuples.get(i);
			String[] arrTuplePosition = tuplePosition.split("-",2);
			String pageName = arrTuplePosition[0];
			int tupleNumber = Integer.parseInt(arrTuplePosition[1]);
			// delete from page and index
			Page currPage = (Page) DBApp.deserializeData(this.filepath + pageName);
			deleteFromIndex(currPage.tuples.get(tupleNumber),pageName);
			currPage.tuples.removeElementAt(tupleNumber);
			if(currPage.tuples.isEmpty())
			{
				this.pageNames.removeElement(pageName);
				File pageFilePath = new File(this.filepath + pageName + ".ser");
				pageFilePath.delete();
				this.serializeTable();
			}
			else
				currPage = currPage.serializeAndDeletePage();
			
			
		}
	}
	
	// ----------------------------- HELPER ------------
	private Index getIndexWithIndexName(String indexName) throws ClassNotFoundException
	{
		
		return (Index) DBApp.deserializeData(this.filepath + "indices/" + indexName);
	}

	private Index getIndexWithColName (String colName) throws ClassNotFoundException, IOException
	{
		List<List<String>> tableInfo = DBApp.getColumnData(this.name);
		for(int i = 0;i<tableInfo.size();i++)
		{
			if(tableInfo.get(i).get(1).equals(colName) && !tableInfo.get(i).get(4).equals("null"))
				return (Index) DBApp.deserializeData(this.filepath + "indices/" + tableInfo.get(i).get(4));
				
		}
		return null;
	}


	public void serializeTable()
	{
		try {
			FileOutputStream file = new FileOutputStream(this.filepath + "info.ser");
			ObjectOutputStream out = new ObjectOutputStream(file);
			out.writeObject(this);
			out.close();
			file.close();
		} catch (IOException e) {
			System.out.println("Failed to serialize table!");
			e.printStackTrace();
		}

	}

	public Table serializeAndDeleteTable()
	{
		serializeTable();
		return null;
	}


}
