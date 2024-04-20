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

	//------------------------------------------------------CREATE INDEX------------------------------------------------------------------
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

	//------------------------------------------------------INSERT------------------------------------------------------------------

	public void insertTupleIntoTable(Tuple tuple, String clusteringKeyColName) throws DBAppException, ClassNotFoundException, IOException
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
			Index clusteringIndex = getIndexWithColName(clusteringKeyColName);
			Page pageToInsertInto;
			// if an index exists, use it to find the page to insert to
			if(clusteringIndex==null)
				pageToInsertInto=findPageToInsert(tuple);
			else 
				pageToInsertInto=findPageToInsertIndex(tuple,clusteringIndex);
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

	private Page findPageToInsertIndex(Tuple tuple,Index clusteringIndex) throws ClassNotFoundException, DBAppException{
		Object clusteringValue = tuple.Primary_key;
		// search for all tuples >= my primary key, and take the first result's page
		// if it is empty then I am to be inserted at the very last page
		ArrayList<Vector<String>> pageNames = clusteringIndex.searchGreaterThan(new Datatype(clusteringValue), true);
		if (pageNames.isEmpty())
			return (Page) DBApp.deserializeData(this.filepath + this.pageNames.lastElement() + ".ser");
		else
			return (Page) DBApp.deserializeData(this.filepath + pageNames.get(0).get(0));	
	}
	private void insertIntoPage(Tuple tuple, Page page) throws DBAppException, ClassNotFoundException
	{
		// binary search for its location inside the page
		// gives us two (or one) possible locations we can insert into 
		int left = 0;
		int right = 0;
		//		if(!page.tuples.isEmpty()) // ??
		right = page.tuples.size()-1;
		int middle;
		while(right-left>1) 
		{
			middle = (right + left)/2;
			if(tuple.compareTo(page.tuples.get(middle))<0)
				right = middle - 1; 
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
			// delete old page information from all indices for this table
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
			// insert new page information for all indices in this table
			insertIntoIndex(tmp, currPage.name);
		}
		currPage = currPage.serializeAndDeletePage();


	}
	private void deleteFromIndex(Tuple tuple, String pageName) throws ClassNotFoundException
	{
		if(this.indexNames.isEmpty())
			return;
		// get all indices and delete the tuple's page from the indices
		for(String indexName : this.indexNames)
		{
			Index index = getIndexWithIndexName(indexName);
			Object tupleData = tuple.entry.get(index.columnName);
			index.deleteFromIndex(new Datatype(tupleData), pageName);
			index = index.serializeAndDeleteIndex();
		}

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
	public void updateTuple(Object clusteringKeyValue, Hashtable<String, Object> htblColNameValue, String primaryKeyColName) throws ClassNotFoundException, DBAppException, IOException
	{
		if(this.pageNames.isEmpty()) //empty table aslan; nothing to update
			return;
		Page pageToUpdate;
		Index index = getIndexWithColName(primaryKeyColName);
		if(index == null)
			pageToUpdate = findPageBinarySearch(clusteringKeyValue);
		else
			pageToUpdate = findPageToUpdateIndex(clusteringKeyValue,index);
		if(pageToUpdate==null) // Page will be null for only one reason: I did not find it in the index, meaning this value does not exist
			return;
		updateTupleInPage(pageToUpdate,clusteringKeyValue,htblColNameValue);
	}

	private Page findPageBinarySearch(Object clusteringKeyValue) throws ClassNotFoundException, DBAppException
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

	private Page findPageToUpdateIndex(Object clusteringKeyValue,Index index) throws ClassNotFoundException, DBAppException{
		Vector <String> resultPages= index.searchIndex(new Datatype(clusteringKeyValue));
		if(resultPages==null)
			return null;
		String pageName = resultPages.get(0);
		Page page = (Page) DBApp.deserializeData(this.filepath + pageName);
		return page;
	}

	private void updateTupleInPage(Page page,Object clusteringKeyValue, Hashtable<String, Object>htblColNameValue) throws IOException, ClassNotFoundException {
		Datatype clusteringKeyValueDatatype = new Datatype(clusteringKeyValue);
		int left = 0;
		int right =page.tuples.size()-1;
		Vector<Tuple> pageTuples =  page.tuples;
		while(left<=right)
		{
			int middle = (left+right)/2;
			Tuple middleTuple = pageTuples.get(middle);
			Datatype middleTupleDatatype = new Datatype(middleTuple.Primary_key);
			if(middleTupleDatatype.compareTo(clusteringKeyValueDatatype)==0)
			{// 1) update index
				// 2) update row
				for(Map.Entry<String, Object> updateEntry : htblColNameValue.entrySet())
				{
					String colName = updateEntry.getKey();
					Object updatedColValue = updateEntry.getValue();
					Index colIndex = getIndexWithColName(colName);
					// update index if exists for this specific column
					if(colIndex!=null)
					{
						Object oldColValue = middleTuple.entry.get(colName);
						colIndex.deleteFromIndex(new Datatype(oldColValue), page.name);
						colIndex.insertIntoIndex(new Datatype(updatedColValue), page.name);
					}
					middleTuple.entry.put(colName, updatedColValue);
				}
				System.out.println("Successfully updated tuple with primary key " + clusteringKeyValue);
				page = page.serializeAndDeletePage();
				return;
			}
			else if(middleTupleDatatype.compareTo(clusteringKeyValueDatatype)>0)
				right = middle-1;
			else
				left = middle+1;

		}
	}


	// ----------------------------- DELETE --------------------------------------------

	public void deleteFromTable(Hashtable<String, Object> htblColNameValue) throws ClassNotFoundException, IOException, DBAppException
	{
		ArrayList<String> resultSoFar = new ArrayList<String>();
		Iterator<Map.Entry <String,Object>> colNameValueIterator = htblColNameValue.entrySet().iterator();
		boolean firstDeletion = true;

		if(!colNameValueIterator.hasNext())// in other words, there is no where in this SQL statement, so delete everything
		{
			deleteAllTuples();
			return;
		}
		// iterate on deletion conditions and get page name + tuple position and intersect everytime
		while(colNameValueIterator.hasNext())
		{
			Map.Entry<String,Object> deletionCondition = colNameValueIterator.next();
			ArrayList<String> deletionConditionResult;
			Index colIndex = getIndexWithColName(deletionCondition.getKey()); 
			// checks if it has an index to use; if not checks if it the pk so we can binary search
			// if not, linear search 3ala table 3ady
			if(colIndex==null)
				deletionConditionResult = findTuplesSatsifyingDeletion(deletionCondition);
			else if(isClustering(deletionCondition.getKey())==true)
				deletionConditionResult = findTuplesSatsifyingDeletionBinarySearch(deletionCondition);
			else
				deletionConditionResult = findTuplesSatsifyingDeletionIndex(deletionCondition,colIndex);
			if(firstDeletion)
			{
				resultSoFar = deletionConditionResult;
				firstDeletion = false;
			}
			else
				resultSoFar = intersect(resultSoFar,deletionConditionResult); 
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
		if(pageResult==null) // no tuples satisfy the condition; return empty result
			return result;

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
	private ArrayList<String> findTuplesSatsifyingDeletionBinarySearch(Map.Entry<String, Object> deletionCondition) throws ClassNotFoundException, DBAppException
	{
		// even though only one result will show up, i will put it in arraylist for ease of use
		ArrayList<String> result = new ArrayList<String>();
		Page deletePage = findPageBinarySearch(deletionCondition.getValue());
		String tuplePostion = getTuplePositionFromPageUsingClusteringKey(deletionCondition.getValue(), deletePage);
		if(tuplePostion==null) // if the tuple was not found in the page; return an empty arraylist
			return result;
		result.add(deletePage.name + "-" + tuplePostion);
		return result;
	}
	// binary search for tuple position
	private String getTuplePositionFromPageUsingClusteringKey(Object clusteringKeyValue, Page page)
	{
		Datatype clusteringKeyValueDatatype = new Datatype(clusteringKeyValue);
		int left = 0;
		int right =page.tuples.size()-1;
		Vector<Tuple> pageTuples =  page.tuples;
		while(left<=right)
		{
			int middle = (left+right)/2;
			Datatype middleTuple = new Datatype(pageTuples.get(middle).Primary_key);
			if(middleTuple.compareTo(clusteringKeyValueDatatype)==0)
				return middle + "";
			else if(middleTuple.compareTo(clusteringKeyValueDatatype)>0)
				right = middle-1;
			else
				left = middle+1;

		}
		return null;

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
		System.out.println("Deleted " + deletionTuples.size() + " tuples");
	}

	private void deleteAllTuples() throws ClassNotFoundException {
		for(int i = 0;i<this.pageNames.size();i++)
		{
			String currentPageName = this.pageNames.get(i);
			File pageFilePath = new File(this.filepath + currentPageName + ".ser");
			pageFilePath.delete();
		}
		this.pageNames.clear();
		this.serializeTable();
		System.out.println("All tuples were deleted");
	}
	//-------------------------------------SELECT-------------------------------------------------------
	public Iterator selectTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws ClassNotFoundException, DBAppException, IOException 
	{
		ArrayList<String> strops = new ArrayList<String>();
		for(int i=0;i<strarrOperators.length;i++) 
			strops.add(strarrOperators[i]);//Arraylist storing operators instead of array(easier for code)

		for(int i=0;i<strarrOperators.length;i++) 
		{
			String strop=strarrOperators[i];
			if(!(strop.equals("AND")||strop.equals("OR")||strop.equals("XOR")))
				throw new DBAppException("Invalid operator (AND,OR,XOR)");
		}
		ArrayList<ArrayList<Tuple>> results=new ArrayList<ArrayList<Tuple>>();
		for(int i=0;i<arrSQLTerms.length;i++) 
		{
			String table_Name = arrSQLTerms[i]._strTableName;
			String col_Name = arrSQLTerms[i]._strColumnName;
			String operator = arrSQLTerms[i]._strOperator;
			Object obj = arrSQLTerms[i]._objValue;

			Table kamal = this;

			List<List<String>> tableInfo = DBApp.getColumnData(table_Name);//Gets data from csv file of table
			ArrayList<String> colTableNames = DBApp.getColumnNames(tableInfo);
			for(int j = 0;i<colTableNames.size();j++)
			{
				if(colTableNames.contains(col_Name))
					break;
				if(j==colTableNames.size()-1)
					throw new DBAppException("Column name not found");
			}
			//>, >=, <, <=, != or = 
			if(!(operator.equals(">") || operator.equals(">=") || operator.equals("<")
					|| operator.equals("<=") || operator.equals("!=") || operator.equals("=")))
				throw new DBAppException("Invalid operator");
			String obj_class=obj.getClass().getName();
			if(!(obj_class=="java.lang.String" ||obj_class=="java.lang.Integer" ||obj_class=="java.lang.Double") )
				throw new DBAppException("Invalid datatype");

			//Exceptionssssss


			boolean flag=true;

			for(int k=0;k<tableInfo.size();k++) {
				if(tableInfo.get(k).get(0).equals(table_Name) && tableInfo.get(k).get(1).equals(col_Name) && tableInfo.get(k).get(5).equals("B+tree")) { 
					selectWithIndex(kamal,col_Name,operator,obj,results);
					flag=false;
				}
				//Checks if current coloumn in statement used has index or not to use which method


			}
			if(flag==true)
				selectWithNoIndex(kamal,col_Name,operator,obj,results);

			//EXCEPTIONS-----------------------------------------------------------------------------------------
		}
		//After getting the result we make following operations according to certain order AND,OR,XOR
		while(!strops.isEmpty()) 
		{
			int indexAND=strops.indexOf("AND");
			int indexOR=strops.indexOf("OR");
			int indexXOR=strops.indexOf("XOR");

			if(indexAND!=-1) 
			{
				ArrayList<Tuple> andResult = intersectArray(results.get(indexAND),results.get(indexAND+1));
				results.remove(indexAND);
				results.add(indexAND,andResult);
				results.remove(indexAND+1);
				strops.remove(indexAND);
			}else if(indexOR!=-1) 
			{	
				ArrayList<Tuple> orResult = union(results.get(indexOR),results.get(indexOR+1));
				results.remove(indexOR);
				results.add(indexOR,orResult);
				results.remove(indexOR+1);
				strops.remove(indexOR);
			}else if (indexXOR!=-1)
			{	
				ArrayList<Tuple> xorResult = XOR(results.get(indexXOR),results.get(indexXOR+1));
				results.remove(indexXOR);
				results.add(indexXOR,xorResult);
				results.remove(indexXOR+1);
				strops.remove(indexXOR);
			}


		}
		//Logic better explained on paper
		//Imp note that now only 1 arraylist left in result which is after doing all required operations
		ArrayList<Tuple> final_result = new ArrayList<Tuple>();
		for(int i=0;i<results.size();i++) 
			final_result.addAll(results.get(i));

		//Remaining array list in first element after doing all operations 
		return final_result.iterator();
	}

	public Vector<String> linearSearch(Table kamal,Datatype target,String col_Name) throws ClassNotFoundException{
		Vector<String> result=new Vector<String>();
		for(String pageName:kamal.pageNames) {
			Page page = (Page) DBApp.deserializeData(kamal.filepath + pageName);
			for(Tuple tuple:page.tuples) {
				Object tuplevalue=tuple.entry.get(col_Name);
				if(target.compareTo(new Datatype(tuplevalue))!=0)
					result.add(pageName);
			}
		}
		LinkedHashSet<String> hashSet = new LinkedHashSet<String>(result);  
		result.clear(); 
		result.addAll(hashSet); 
		return result;

	}

	public void selectWithNoIndex (Table kamal,String col_Name,String operator,Object obj,ArrayList<ArrayList<Tuple>> results) throws ClassNotFoundException, IOException, DBAppException 
	{
		List<List<String>> tableInfo = DBApp.getColumnData(kamal.name);//Gets data from csv file of table

		//		ArrayList<String> colTableNames = DBApp.getColumnNames(tableInfo);
		String primKey=DBApp.getPrimaryKeyName(tableInfo);

		ArrayList<Tuple> conditionSatisfied = new ArrayList<Tuple>();
		if(!col_Name.equals(primKey)) 
		{ //First check if colomn is not primary key do linear search
			for(String pageName:kamal.pageNames)
			{
				Page page = (Page) DBApp.deserializeData(kamal.filepath + pageName);
				for(int j = 0 ; j<page.tuples.size();j++) 
				{
					Tuple tupleSearch=page.tuples.get(j);
					Tuple t = makeConditionList(tupleSearch,col_Name,operator,obj);
					if(t!=null)
						conditionSatisfied.add(t);
				}

			}//gets each page from searchPages then each tuple in that page
			//Check if condition satisfied add to arraylist of tuples conditionIndex
		}
		else //Colomn is primary key so must use binary search
		{
			Page pageToSelect=null;
			Tuple targetTuple=null;
			int startPagenum=0;
			int tuplenum=0;
			int pageSize=0;
			switch(operator) 
			{
			case "=":
				pageToSelect=findPageBinarySearch(obj);
				targetTuple=getTupleFromPageUsingClusteringKey(obj,pageToSelect);

				if(targetTuple!=null)
					conditionSatisfied.add(targetTuple);break;
			case "!=":
				for(String pageName:kamal.pageNames)
				{
					Page page = (Page) DBApp.deserializeData(kamal.filepath + pageName);
					for(int j = 0 ; j<page.tuples.size();j++) 
					{
						Tuple tupleSearch=page.tuples.get(j);
						Tuple t = makeConditionList(tupleSearch,col_Name,operator,obj);
						if(t!=null)
							conditionSatisfied.add(t);
					}

				}break;
				//Linear search on tuples to find which satisfy condition
			case "<":
				pageToSelect=findPageBinarySearch(obj);
				targetTuple=getTupleFromPageUsingClusteringKey(obj,pageToSelect);
				startPagenum=pageToSelect.num;

				pageSize=pageToSelect.maxNoEnteries;
				tuplenum=(int)obj%pageSize;//To get number of which tuple in page
				for(int j = tuplenum ; j>=0;j--) 
				{
					Tuple tupleSearch=pageToSelect.tuples.get(j);
					Tuple t = makeConditionList(tupleSearch,col_Name,operator,obj);
					if(t!=null)
						conditionSatisfied.add(t);
				}//First get page and tuple to start searching from by binary search
				//Then by linear search on the rest of the tuples in that page find satisfying tuples 


				for(int i=startPagenum-1;i>=0;i--) 
				{
					String pageName=pageNames.get(i);
					Page page = (Page) DBApp.deserializeData(kamal.filepath + pageName);
					for(int j = page.tuples.size()-1 ; j>=0;j--) 
					{
						Tuple tupleSearch=page.tuples.get(j);
						Tuple t = makeConditionList(tupleSearch,col_Name,operator,obj);
						if(t!=null)
							conditionSatisfied.add(t);
					}
				}break;
				//Then complete remaining pages and tuples to find all satisfying conditions
			case "<=":
				pageToSelect=findPageBinarySearch(obj);
				targetTuple=getTupleFromPageUsingClusteringKey(obj,pageToSelect);
				startPagenum=pageToSelect.num;

				pageSize=pageToSelect.maxNoEnteries;
				tuplenum=(int)obj%pageSize;//To get number of which tuple in page

				if(targetTuple!=null)
					conditionSatisfied.add(targetTuple);
				for(int j = tuplenum-1 ; j>=0;j--) 
				{
					Tuple tupleSearch=pageToSelect.tuples.get(j);
					Tuple t = makeConditionList(tupleSearch,col_Name,operator,obj);
					if(t!=null)
						conditionSatisfied.add(t);
				}//First get page and tuple to start searching from by binary search
				//Then by linear search on the rest of the tuples in that page find satisfying tuples 

				for(int i=startPagenum-1;i>=0;i--) 
				{
					String pageName=pageNames.get(i);
					Page page = (Page) DBApp.deserializeData(kamal.filepath + pageName);
					for(int j = page.tuples.size()-1 ; j>=0;j--) 
					{
						Tuple tupleSearch=page.tuples.get(j);
						Tuple t = makeConditionList(tupleSearch,col_Name,operator,obj);
						if(t!=null)
							conditionSatisfied.add(t);
					}
				}break;
				//Complete remaining pages and tuples linear
			case ">":
				pageToSelect=findPageBinarySearch(obj);
				targetTuple=getTupleFromPageUsingClusteringKey(obj,pageToSelect);
				startPagenum=pageToSelect.num;

				pageSize=pageToSelect.maxNoEnteries;
				tuplenum=(int)obj%pageSize;//To get number of which tuple in page

				for(int j = tuplenum ; j<pageToSelect.tuples.size();j++) 
				{
					Tuple tupleSearch=pageToSelect.tuples.get(j);
					Tuple t = makeConditionList(tupleSearch,col_Name,operator,obj);
					if(t!=null)
						conditionSatisfied.add(t);
				}
				//First get page and tuple to start searching from by binary search
				//Then by linear search on the rest of the tuples in that page find satisfying tuples 

				for(int i=startPagenum+1;i<kamal.pageNames.size();i++) 
				{
					String pageName=pageNames.get(i);
					Page page = (Page) DBApp.deserializeData(kamal.filepath + pageName);
					for(int j = 0 ; j<page.tuples.size();j++) 
					{
						Tuple tupleSearch=page.tuples.get(j);
						Tuple t = makeConditionList(tupleSearch,col_Name,operator,obj);
						if(t!=null)
							conditionSatisfied.add(t);
					}
				}break;
				//Complete remaining pages and tuples linear
			case ">=":
				pageToSelect=findPageBinarySearch(obj);
				targetTuple=getTupleFromPageUsingClusteringKey(obj,pageToSelect);
				startPagenum=pageToSelect.num;

				pageSize=pageToSelect.maxNoEnteries;
				tuplenum=(int)obj%pageSize;//To get number of which tuple in page

				if(targetTuple!=null)
					conditionSatisfied.add(targetTuple);

				for(int j = tuplenum+1 ; j<pageToSelect.tuples.size();j++) 
				{
					Tuple tupleSearch=pageToSelect.tuples.get(j);
					Tuple t = makeConditionList(tupleSearch,col_Name,operator,obj);
					if(t!=null)
						conditionSatisfied.add(t);
				}
				//First get page and tuple to start searching from by binary search
				//Then by linear search on the rest of the tuples in that page find satisfying tuples 

				for(int i=startPagenum+1;i<kamal.pageNames.size();i++) 
				{
					String pageName=pageNames.get(i);
					Page page = (Page) DBApp.deserializeData(kamal.filepath + pageName);
					for(int j = 0 ; j<page.tuples.size();j++) 
					{
						Tuple tupleSearch=page.tuples.get(j);
						Tuple t = makeConditionList(tupleSearch,col_Name,operator,obj);
						if(t!=null)
							conditionSatisfied.add(t);
					}
				}break;
				//Complete remaining pages and tuples linear
			default:System.out.println("NOT approprtaite operator");break;
			}


		}
		results.add(conditionSatisfied);
		//add arraylist of tuples satisfying condition to result
	}


	public void selectWithIndex (Table kamal,String col_Name,String operator,Object obj,ArrayList<ArrayList<Tuple>> results) throws ClassNotFoundException, IOException 
	{
		ArrayList<Tuple> conditionIndex = new ArrayList<Tuple>();
		Index colIndex = getIndexWithColName(col_Name);
		Datatype obj2 = new Datatype(obj);
		Vector<String> searchPages = new Vector<String>();
		ArrayList<Vector<String>> searchPagesRange=new ArrayList<Vector<String>>();
		//Must use searchPagesRange because the output of using greaterthan,smallerthan,etc

		switch(operator) 
		{
		case "=":searchPages=colIndex.searchIndex(obj2);break;

		case "!=":searchPages=linearSearch(kamal,obj2,col_Name);break;//Linear search

		case "<":searchPagesRange=colIndex.searchLessThan(obj2,false);break;

		case "<=":searchPagesRange=colIndex.searchLessThan(obj2,true);break;

		case ">":searchPagesRange=colIndex.searchGreaterThan(obj2,false);break;

		case ">=":searchPagesRange=colIndex.searchGreaterThan(obj2,true);break;
		// TODO enta kamn mesh 3aref tespell 
		//Bas yaala 5alek fe sho8lak
		default:System.out.println("Cannot find appropriaite operator");break;
		}
		if(!searchPagesRange.isEmpty()) {
			for(int i=0;i<searchPagesRange.size();i++) {
				Vector<String> temp=searchPagesRange.get(i);
				for(int j=0;j<temp.size();j++) 
					searchPages.add(temp.get(j));
			}
		}
		//Makes searchPagesRange a vactor of strings showing pages to where values have been found
		//Alot of duplicates

		if(searchPages!=null)
		{
			LinkedHashSet<String> hashSet = new LinkedHashSet<String>(searchPages);  
			searchPages.clear(); 
			searchPages.addAll(hashSet); 
		}
		else 
			searchPages = new Vector<String>();
		//Remove all duplicates from searchPages

		for(String pageName:searchPages) {
			Page page = (Page) DBApp.deserializeData(kamal.filepath + pageName);
			for(int j = 0 ; j<page.tuples.size();j++) 
			{
				Tuple tupleSearch=page.tuples.get(j);
				Tuple t = makeConditionList(tupleSearch,col_Name,operator,obj);
				if(t!=null)
					conditionIndex.add(t);
			}

		}
		//gets each page from searchPages then each tuple in that page
		//Check if condition satisfied add to arraylist of tuples conditionIndex

		results.add(conditionIndex);
		//Then add conditionIndex to result which is arraylist of arraylists of tuples.
	}

	private Tuple getTupleFromPageUsingClusteringKey(Object clusteringKeyValue, Page page)
	{
		Datatype clusteringKeyValueDatatype = new Datatype(clusteringKeyValue);
		int left = 0;
		int right =page.tuples.size()-1;
		Vector<Tuple> pageTuples =  page.tuples;
		while(left<=right)
		{
			int middle = (left+right)/2;
			Datatype middleTuple = new Datatype(pageTuples.get(middle).Primary_key);
			if(middleTuple.compareTo(clusteringKeyValueDatatype)==0)
				return page.tuples.get(middle);
			else if(middleTuple.compareTo(clusteringKeyValueDatatype)>0)
				right = middle-1;
			else
				left = middle+1;

		}
		return null;

	}

	public Tuple makeConditionList(Tuple tupleSearch,String col_Name,String operator,Object obj)
	{
		Tuple temp = null;

		Datatype tableValue2=new Datatype(tupleSearch.entry.get(col_Name));
		Datatype obj2=new Datatype(obj);

		switch(operator) 
		{
		case "=":
			if(tableValue2.compareTo(obj2)==0){
				return (tupleSearch);
			}break;
		case "!=":
			if(tableValue2.compareTo(obj2)!=0){
				return (tupleSearch);
			}break;
		case "<":
			if(tableValue2.compareTo(obj2)<0){
				return (tupleSearch);
			}break;
		case "<=":
			if(tableValue2.compareTo(obj2)<=0){
				return(tupleSearch);
			}break;
		case ">":
			if(tableValue2.compareTo(obj2)>0){
				return(tupleSearch);
			}break;
		case ">=":
			if(tableValue2.compareTo(obj2)>=0){
				return(tupleSearch);
			}break;
		default:System.out.println("Cannot find approprtaite operator");break;
		}


		return temp;
	}

	private ArrayList<Tuple> intersectArray(ArrayList<Tuple> firstList, ArrayList<Tuple> secondList)
	{
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		for(Tuple currentPosition:firstList)
			if(secondList.contains(currentPosition))
				result.add(currentPosition);
		return result;


	}
	private ArrayList<Tuple> union(ArrayList<Tuple> firstList, ArrayList<Tuple> secondList)
	{
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		result.addAll(firstList);  

		for(int i = 0;i<secondList.size();i++)
			if(!result.contains(secondList.get(i)))
				result.add(secondList.get(i));


		return result;
	}
	private ArrayList<Tuple> XOR(ArrayList<Tuple> firstList, ArrayList<Tuple> secondList)

	{
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		for (Tuple element : firstList) {
			if (!secondList.contains(element)) {
				result.add(element);
			}
		}
		for (Tuple element : secondList) {
			if (!firstList.contains(element)) {
				result.add(element);
			}
		}
		return result;
		// ----------------------------- HELPER ------------
	}
	public Index getIndexWithIndexName(String indexName) throws ClassNotFoundException
	{

		return (Index) DBApp.deserializeData(this.filepath + "indices/" + indexName);
	}

	public Index getIndexWithColName (String colName) throws ClassNotFoundException, IOException
	{
		List<List<String>> tableInfo = DBApp.getColumnData(this.name);
		for(int i = 0;i<tableInfo.size();i++)
		{
			if(tableInfo.get(i).get(1).equals(colName) && !tableInfo.get(i).get(4).equals("null"))
				return (Index) DBApp.deserializeData(this.filepath + "indices/" + tableInfo.get(i).get(4));

		}
		return null;
	}

	private boolean isClustering(String colName) throws IOException
	{
		List<List<String>> tableInfo = DBApp.getColumnData(this.name);
		for(int i =0;i<tableInfo.size();i++)
			if(tableInfo.get(i).get(1).equals(colName) && tableInfo.get(i).get(4).equals("True"))
				return true;
		return false;

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
