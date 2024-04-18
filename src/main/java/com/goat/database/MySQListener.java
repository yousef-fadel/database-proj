package com.goat.database;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import com.goat.bonus.MySqlLexer;
import com.goat.bonus.MySqlParser;
import com.goat.bonus.MySqlParser.CreateDefinitionContext;
import com.goat.bonus.MySqlParser.ExpressionsWithDefaultsContext;
import com.goat.bonus.MySqlParserBaseListener;



public class MySQListener extends MySqlParserBaseListener
{
	DBApp database;
	Iterator result = null;

	public MySQListener() throws ClassNotFoundException, DBAppException
	{
		database = new DBApp();
	}

	public Iterator parse(StringBuffer strBufferSQL)
	{
		// charstream howa hay5od el input; kol el ta7t estanba
		CharStream inputStream = CharStreams.fromString(strBufferSQL.toString());
		MySqlLexer cookieLexer = new MySqlLexer(inputStream);
		CommonTokenStream commonTokenStream = new CommonTokenStream(cookieLexer);
		MySqlParser cookieParser = new MySqlParser(commonTokenStream);
		// da bey3mel parsetree baydatan mein el root
		ParseTree tree = cookieParser.root();
		// law tel3 eorr visiterrornode haysht8ala, 8eir keda 3ala 7asab el string haysha8al el method bet3atha
		ParseTreeWalker.DEFAULT.walk(this, tree);
		return this.result;
	}

	// TODO what about autoincrement? throw exception also?
	public void enterColumnCreateTable(MySqlParser.ColumnCreateTableContext ctx) 
	{
		String tableName = ctx.tableName().getText();
		String clusteringKey = "";
		// list containing all columns + datatype + primary key
		List<CreateDefinitionContext> columnInformation = ctx.createDefinitions().createDefinition();
		Hashtable<String,String> htblColNameType = new Hashtable<String, String>();
		for(CreateDefinitionContext column : columnInformation)
		{
			String colName = column.getChild(0).getText();
			String colType = getDataType(column.getChild(1).getChild(0).getText());
			htblColNameType.put(colName, colType);
			String colExtraData = column.getChild(1).getText(); //da ay 7aga zeyad, zay not null w primary key
			if(colExtraData.toLowerCase().contains("primarykey"))
				clusteringKey = colName;
		}
		try {
			database.createTable(tableName, clusteringKey, htblColNameType);
		} catch (ClassNotFoundException | DBAppException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void enterCreateIndex(MySqlParser.CreateIndexContext ctx) 
	{
		String tableName = ctx.tableName().getText();
		String indexName = ctx.children.get(2).getText(); //TODO there has to be a better way to get index name
		if(ctx.indexColumnNames().indexColumnName().size()>1)
			throw new RuntimeException(new DBAppException("Unsupported SQL statement"));
		String colName = ctx.indexColumnNames().indexColumnName(0).getText();
		try {
			database.createIndex(tableName, colName, indexName);
		} catch (ClassNotFoundException | DBAppException | IOException e) {
			throw new RuntimeException(e);
		}
	}		

	public void enterInsertStatement(MySqlParser.InsertStatementContext ctx) 
	{
		// syntax is insert into table (col1,col2) values (value1,value2),etc...;
		if(ctx.columns==null)
			throw new RuntimeException(new DBAppException("Unsupported/Wrong SQL statement"));
		String tableName = ctx.tableName().getText();
		String[] columnsToBeInserted = ctx.columns.getText().split(",");
		Hashtable<String,Object> htblColNameValue = new Hashtable<String, Object>();

		List<ExpressionsWithDefaultsContext> rowsInserted = ctx.insertStatementValue().expressionsWithDefaults();
		for(ExpressionsWithDefaultsContext row : rowsInserted)
		{
			htblColNameValue.clear();
			String[] columnValues = row.getText().split(",");
			for(int j = 0;j<columnsToBeInserted.length;j++)
			{
				String colName = columnsToBeInserted[j];
				Object colValue = parseValue(columnValues[j]);
				htblColNameValue.put(colName, colValue);
			}
			try {
				database.insertIntoTable(tableName, htblColNameValue);
			} catch (ClassNotFoundException | DBAppException | IOException e) {
				throw new RuntimeException(e);
			}
		}
	}


	// turns datatype entered to the one we use in the project
	// ex: integer becomes java.lang.Integer
	private String getDataType(String datatypeName)
	{
		datatypeName = datatypeName.toLowerCase();
		if(datatypeName.equals("int"))
			return "java.lang.Integer";
		if(datatypeName.contains("char"))
			return "java.lang.String";
		if(datatypeName.contains("double"))
			return "java.lang.Double";
		return "i am an unsupported datatype"; // will throw exception when creating table; da asdy

	}
	
	// given a string value, turns it into what it seems to be 
	// if the value is between '', then a string is returned, if it has a . then a double is returned
	// o.w return integer
	private Object parseValue(String value)
	{
		if(value.charAt(0)=='\'')
		{
			value.replaceAll("'", "");
			return new String(value);
		}
		else if(value.contains("."))
			return new Double(Double.parseDouble(value));
		else
			return new Integer(Integer.parseInt(value));
	}
	
	public void visitErrorNode(ErrorNode node)
	{
		throw new RuntimeException(new DBAppException("Unsupported/Wrong SQL statement"));
	}
}
