package com.goat.database;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

import com.goat.btree.BTree;
// TODO index should be saved in tables/TableName/indices/IndexName
// TODO index fannout should be gotten from config file
public class Index implements Serializable{
	String name;
	String columnName;
	String indexFilepath;
	BTree<Datatype, Vector<String>> btree;

	public Index(String name, String columnName,String tableFilepath)
	{
		this.name = name;
		this.indexFilepath = tableFilepath+"indices/" + name+".ser";
		this.columnName = columnName;
		btree = new BTree<Datatype,Vector<String>>();
	}
	
	public void insertIntoIndex(Datatype datatype,String page)
	{
		Vector<String> tuplePositions;
		tuplePositions = btree.search(datatype);
		if(tuplePositions==null)
		{
			tuplePositions = new Vector<String>();
			tuplePositions.add(page);
			btree.insert(datatype, tuplePositions);
		}
		else
		{
			tuplePositions.add(page);
			btree.delete(datatype);
			btree.insert(datatype, tuplePositions);
		}
		
	}
	
	public void deleteFromIndex(Datatype datatype, String page)
	{
		Vector<String> tuplePositions = this.searchIndex(datatype);
		if(tuplePositions!=null)
		{
			btree.delete(datatype);
			tuplePositions.remove(page);
			if(tuplePositions.size()!=0)
				btree.insert(datatype, tuplePositions);
		}
	}
	
	public Vector<String> searchIndex(Datatype datatype)
	{
		return btree.search(datatype);
	}
	
	//should return list of pages
	public ArrayList<Vector<String>> searchGreaterThan(Datatype datatype,boolean inclusive)
	{
		return btree.searchGreaterThan(datatype, inclusive);
	}

	public ArrayList<Vector<String>> searchLessThan(Datatype datatype,boolean inclusive)
	{
		return btree.searchLessThan(datatype, inclusive);
	}
	
	// save index 3ady
	public void serializeIndex()
	{
		try {
			FileOutputStream file = new FileOutputStream(indexFilepath);
			ObjectOutputStream out = new ObjectOutputStream(file);
			out.writeObject(this);
			out.close();
			file.close();
		} catch (IOException e) {
			System.out.println("Failed to serialize index!");
			e.printStackTrace();
		}
	}
	
	public Index serializeAndDeleteIndex()
	{
		this.serializeIndex();
		return null;
	}

	public static void main(String[]args)
	{	
		Vector<String> x = new Vector<String>();
	}
}
