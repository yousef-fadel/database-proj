package com.goat.database;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Vector;

public class Table implements java.io.Serializable{
	public Vector<String> pageNames; //
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
			// if we reach the end of our pages, create a new page and store the last element in it
			// otherwise, save our current page and move onto the next page and insert the tmp at the top
			if(this.pageNames.lastElement().equals(currPage.name))
			{
				Page newPage = new Page(this.name + (this.numberForPage), this.numberForPage++, this.filepath);
				this.pageNames.add(newPage.name);
				newPage.tuples.add(tmp);
				newPage = newPage.serializeAndDeletePage();
				this.serializeTable();
				break;
			}
			currPage.serializePage();
			int nextPage = this.pageNames.indexOf(currPage.name) + 1;
			currPage = (Page) DBApp.deserializeData(this.filepath + this.pageNames.get(nextPage));
			currPage.tuples.insertElementAt(tmp, 0);	
		}
		currPage = currPage.serializeAndDeletePage();


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
