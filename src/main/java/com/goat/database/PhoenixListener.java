package com.goat.database;

import java.util.Hashtable;

import org.antlr.v4.runtime.tree.ErrorNode;

import com.goat.bonus.PhoenixParser;
import com.goat.bonus.PhoenixParser.Column_defContext;
import com.goat.bonus.PhoenixParserBaseListener;


public class PhoenixListener extends PhoenixParserBaseListener
{
	DBApp database;
	public PhoenixListener() throws ClassNotFoundException, DBAppException
	{
		database = new DBApp();
	}
	@Override public void enterCreate_table_command(PhoenixParser.Create_table_commandContext ctx) 
	{
		//get all columns + anything related to them
		//ctx.column_def_list().getText()
		System.out.println(ctx.column_def_list().getText());
		
	}

	@Override public void exitCreate_table_command(PhoenixParser.Create_table_commandContext ctx) 
	{
		String tableName = ctx.table_ref().getText();
		String primaryKey = ctx.column_def_list().column_def(0).column_ref().getText();
		boolean primaryKeyFound = false;
		Hashtable<String,String> tableData = new Hashtable<String, String>();
		for( Column_defContext col : ctx.column_def_list().column_def())
		{
			String datatype = getDataType(col.data_type().getText());
			String colName = col.column_ref().getText();
			System.out.println();
			tableData.put(colName, datatype);
			if(col.getText().contains("primarykey") && primaryKeyFound)
				visitErrorNode(null);// law deeh esht8alt yeb2a ana gamed
			else if(col.getText().contains("primarykey") && !primaryKeyFound)
			{
				primaryKey = colName;
				primaryKeyFound = true;
			}
		}
		try
		{
			database.createTable(tableName, primaryKey, tableData);
		}
		catch(Exception e)
		{
			throw new RuntimeException(e);
		}
	}
	
	// turns datatype entered to the one we use in the project
	// ex: integer becomes java.lang.Integer
	private String getDataType(String datatypeName)
	{
		if(datatypeName.equals("integer"))
			return "java.lang.Integer";
		if(datatypeName.contains("char"))
			return "java.lang.String";
		if(datatypeName.equals("double"))
			return "java.lang.Double";
		return "java.lang.Float"; // will throw exception when creating table; da asdy
			
	}
	public void visitErrorNode(ErrorNode node)
	{
        throw new RuntimeException(new DBAppException("Method not implemented: " + node.getText()));
	}
}
