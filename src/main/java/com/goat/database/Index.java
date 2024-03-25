package com.goat.database;

import java.io.Serializable;
// TODO index should be saved in tables/TableName/indices/IndexName
public class Index implements Serializable{
	String name;
	String filepath;
	
	// index name given by user; filepath hatkoon table.filepath + "/indices/" + this.name + "/"
	public Index(String name, String filepath)
	{
		this.name = name;
		this.filepath = filepath;
	}
	
	public void insertIntoIndex()
	{
		
	}
	
	public void deleteFromIndex()
	{
		
	}
	
	//should return page
	public void searchIndex()
	{
		
	}
	
	//should return list of pages
	public void searchRangeIndex()
	{
		
	}
	
	// save index 3ady
	public void saveIndex()
	{
		
	}

}
