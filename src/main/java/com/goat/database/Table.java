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
		System.out.println(pageNames);
		if(pageNames.isEmpty())
		{
			Page firstPage = new Page(this.name + "1");
			pageNames.add(firstPage.name);
			firstPage.tuples.add(tuple);
			
			serializedata(firstPage, this.filepath + firstPage.name);
			DBApp db =new DBApp();
			Vector<Table> v = (Vector<Table>) deserializeData("./resources/tables"); 
//			v.add(null)
			serializedata(v, "./resources/tables");
		}
		else
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

			Page page1 = (Page) deserializeData(this.filepath + this.name + (leftPage+1));
			System.out.println(this.filepath + this.name + (leftPage+1));
			Page page2 = (Page) deserializeData(this.filepath + this.name + (rightPage+1));
			if(this.pageNames.size()==1)
				insertIntoPage(tuple,page1,leftPage);
			else
			{
				if(tuple.compareTo(page1.tuples.get(page1.tuples.size()-1)) > 0)
					insertIntoPage(tuple,page1,leftPage);
				else
					insertIntoPage(tuple,page2,rightPage);
			}
		}
		System.out.println(pageNames);
		DBApp db =new DBApp();
		Vector<Table> v = (Vector<Table>) deserializeData("./resources/tables"); 
		serializedata(v, "./resources/tables");
	}

	private void insertIntoPage(Tuple tuple, Page page, int pageNumber) throws DBAppException, ClassNotFoundException
	{
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
		if(page.tuples.size()>page.maxNoEnteries)
		{
			shiftTuples(page, pageNumber);
		}
		else
			serializedata(page, this.filepath + page.name);
		
		
	}

	private void shiftTuples(Page currPage, int pageNumber) throws ClassNotFoundException, DBAppException
	{

		while(currPage.tuples.size() > currPage.maxNoEnteries)
		{ 
			Tuple tmp = currPage.tuples.get(currPage.tuples.size()-1);
			currPage.tuples.remove(currPage.tuples.size()-1);
			if(this.pageNames.size()-1==pageNumber)
					{
						Page newPage = new Page(this.name + (this.pageNames.size()+1));
						this.pageNames.add(newPage.name);
						newPage.tuples.add(tmp);
						serializedata(newPage, this.filepath + newPage.name);
						break;
					}
			serializedata(currPage,this.filepath + currPage.name);
			currPage = (Page)deserializeData(this.filepath + this.pageNames.get(++pageNumber));
			currPage.tuples.insertElementAt(tmp, 0);
		}
		DBApp db =new DBApp();
		Vector<Table> v = (Vector<Table>) deserializeData("./resources/tables"); 

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
