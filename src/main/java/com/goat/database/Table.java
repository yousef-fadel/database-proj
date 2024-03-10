package com.goat.database;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Vector;

public class Table implements java.io.Serializable{
	public Vector<String> pageNames; //
	public String filepath; //location all pages
	public String name; //table name



	public Table(String name, String filepath)
	{
		this.name = name;
		this.filepath = filepath;
		this.pageNames = new Vector<String>();
	}

	
	public void insert(Tuple tuple) throws DBAppException, ClassNotFoundException
	{
		// check if this is the first tuple to be inserted in the table; if it is create a page and insert it
		if(pageNames.isEmpty())
		{
			Page firstPage = new Page(this.name + "0", 0);
			pageNames.add(firstPage.name);
			firstPage.tuples.add(tuple);
			
			serializedata(firstPage, this.filepath + firstPage.name);
			serializedata(this, this.filepath + "info");
			
		}
		else
		{
			Page pageToInsertInto = findPageToInsert(tuple);
			insertIntoPage(tuple, pageToInsertInto);
			serializedata(this, this.filepath + "info");
			
		}

	}

	private void insertIntoPage(Tuple tuple, Page page) throws DBAppException, ClassNotFoundException
	{
		// binary search for its location inside the page
		// gives us two possible locations we can insert into
		int left = 0;
		int right = 0;
		if(!page.tuples.isEmpty())
			right = page.tuples.size()-1;
		int middle;
		while(right-left>1)
		{
			middle = left + (right - left)/2;
			if(tuple.compareTo(page.tuples.get(middle))<0)
				right = middle;
			else if (tuple.compareTo(page.tuples.get(middle))>0)
				left = middle;
			else 
				throw new DBAppException("The primary key is a duplicate");
		}
		
		// left and right come from binary search up above
		// check which position to insert into; if it is smaller than both values, then place it where the left it,
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
			serializedata(page, this.filepath + page.name);
		System.out.println(this.pageNames);
		
		
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
			if(this.pageNames.size()-1==currPage.num)
					{
						Page newPage = new Page(this.name + (this.pageNames.size()), this.pageNames.size());
						this.pageNames.add(newPage.name);
						newPage.tuples.add(tmp);
						serializedata(newPage, this.filepath + newPage.name);
						serializedata(this, this.filepath + "info");
						break;
					}
			serializedata(currPage,this.filepath + currPage.name);
			currPage = (Page)deserializeData(this.filepath + this.name +(currPage.num+1));
			currPage.tuples.insertElementAt(tmp, 0);
		}
		serializedata(currPage, this.filepath + currPage.name);


	}

	private Page findPageToInsert(Tuple tuple) throws ClassNotFoundException, DBAppException
	{
		int leftPage = 0;
		int rightPage = pageNames.size()-1;
		int middlePage;
		//binary search for the page; stop once 2 pages are left
		while(leftPage-rightPage>2)
		{
			middlePage = leftPage + (leftPage + rightPage)/2;
			Page currPage = (Page)deserializeData(this.filepath + this.name + middlePage);
			Tuple currTuple = currPage.tuples.get((currPage.tuples.size()/2));
			if(tuple.compareTo(currTuple)<0)
				rightPage = middlePage;
			else if(tuple.compareTo(currTuple)>0)
				leftPage = middlePage;
			else
				throw new DBAppException("The primary key is a duplicate");
		}

		Page page1 = (Page) deserializeData(this.filepath + this.name + (leftPage));
		Page page2 = (Page) deserializeData(this.filepath + this.name + (rightPage));
		if(this.pageNames.size()==1)
			 return page1;
		else
		{
			if(tuple.compareTo(page1.tuples.get(page1.tuples.size()-1)) < 0)
				return page1;
			else
				return page2;
		}
	}
	public void serializedata(Object o, String filename) {
		try {
			FileOutputStream file = new FileOutputStream(filename+".ser");
			ObjectOutputStream out = new ObjectOutputStream(file);
			out.writeObject(o);
			out.close();
			file.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public Object deserializeData(String filename) throws ClassNotFoundException {
		try {

			FileInputStream fileIn = new FileInputStream(filename + ".ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Object output = in.readObject();
			in.close();
			fileIn.close();
			return output;

		} catch (IOException i) {

			return null;
		}
	}
}
