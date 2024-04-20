package com.goat.database;

import java.io.IOException;
import java.util.ArrayList;
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
import com.goat.bonus.MySqlParser.ExpressionContext;
import com.goat.bonus.MySqlParser.ExpressionsWithDefaultsContext;
import com.goat.bonus.MySqlParser.FromClauseContext;
import com.goat.bonus.MySqlParser.QuerySpecificationContext;
import com.goat.bonus.MySqlParser.SingleDeleteStatementContext;
import com.goat.bonus.MySqlParser.SingleUpdateStatementContext;
import com.goat.bonus.MySqlParser.UpdatedElementContext;
import com.goat.bonus.MySqlParserBaseListener;



public class MySQListener extends MySqlParserBaseListener
{
	DBApp database;
	Iterator result = null;

	public MySQListener() throws ClassNotFoundException, DBAppException
	{
		database = new DBApp();
	}
	public void exitCopyCreateTable(MySqlParser.CopyCreateTableContext ctx) {
		System.out.println(ctx);
	}
	
	@Override public void enterQueryCreateTable(MySqlParser.QueryCreateTableContext ctx) { 
		System.out.println(ctx);

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
			if(colExtraData.toLowerCase().contains("auto_increment"))
				throw new RuntimeException(new DBAppException("Unsupported SQL statement"));
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

	public void enterUpdateStatement(MySqlParser.UpdateStatementContext ctx) 
	{
		// ma3ndeesh fekra ya3ni eh multipleupdatestatment; el mohem e7na mesh bensupport it
		if(ctx.multipleUpdateStatement()!=null)
			throw new RuntimeException(new DBAppException("Unsupported/Wrong SQL statement"));

		SingleUpdateStatementContext ctxUpdate = ctx.singleUpdateStatement();
		// makes sure the update is supported by us
		checkUpdateIsSupported(ctxUpdate);

		String tableName = ctxUpdate.tableName().getText();
		String clusteringNameValue []= ctxUpdate.expression().getText().split("=");//contains {primarykeyname, primarykeyValue}
		String strClusteringKeyValue = clusteringNameValue[1];
		if(strClusteringKeyValue.charAt(0)=='\'')
			strClusteringKeyValue = strClusteringKeyValue.replaceAll("'", ""); //this removes ' from strings

		Hashtable<String,Object> htblColNameValue = new Hashtable<String, Object>();
		for(UpdatedElementContext updatedColumn : ctxUpdate.updatedElement())
		{
			String nameValue[] = updatedColumn.getText().split("="); // array contains {name, value}
			htblColNameValue.put(nameValue[0], parseValue(nameValue[1]));
		}
		try {
			database.updateTable(tableName, strClusteringKeyValue, htblColNameValue);
		} catch (ClassNotFoundException | DBAppException | IOException e) {
			throw new RuntimeException(e);
		}

	}

	private void checkUpdateIsSupported(SingleUpdateStatementContext ctx)
	{
		if(ctx.expression() == null) //mafeesh where aslan
			throw new RuntimeException(new DBAppException("Unsupported/Wrong SQL statement"));
		String primaryColumnName = getPrimaryKeyColumn(ctx.tableName().getText());

		// where should contain one condition only and that condition should be the primary key
		if(ctx.expression().children.size()>1 || !ctx.expression().getText().contains(primaryColumnName))
			throw new RuntimeException(new DBAppException("Unsupported/Wrong SQL statement"));

		// set should not contain primary key
		for(UpdatedElementContext updatedColumn : ctx.updatedElement())
			if(updatedColumn.getText().contains(primaryColumnName))
				throw new RuntimeException(new DBAppException("Unsupported/Wrong SQL statement"));
	}

	public void enterDeleteStatement(MySqlParser.DeleteStatementContext ctx) 
	{
		// bardo no idea what multiple delete statement is; unsupported w 5alas
		if(ctx.multipleDeleteStatement()!=null)
			throw new RuntimeException(new DBAppException("Unsupported/Wrong SQL statement"));

		SingleDeleteStatementContext ctxDelete = ctx.singleDeleteStatement();
		checkDeleteIsSupported(ctxDelete.expression());
		String tableName = ctxDelete.tableName().getText();
		Hashtable<String,Object> htblColNameValue = new Hashtable<String, Object>();
		if(ctxDelete.expression()!=null)
			addValuesToDeleteHashTable(htblColNameValue, ctxDelete.expression().getChild(0).getParent());
		try {
			database.deleteFromTable(tableName, htblColNameValue);
		} catch (ClassNotFoundException | DBAppException | IOException e) {
			throw new RuntimeException(e);
		}

	}
	private void checkDeleteIsSupported(ExpressionContext ctx)
	{
		if(ctx==null) //means there is no where condition aslan; 3ady delete all
			return;

		// turns expressioncontext to parsetree
		// here tree can be split into {entire where except last condition, operator, last condition}
		// each can be reached by using getChild(i)
		ParseTree whereClause = ctx.getChild(0).getParent();

		// if one condition we handle it seperately as the way the parser counts the children is weird 
		if(whereClause.getChildCount()==1)
			checkOperator(whereClause);
		else // this checks that all conditions are anded together and we only use = not < or >
			checkOperatorAndConditions(whereClause);
	}
	private void checkOperatorAndConditions(ParseTree whereClause)
	{
		if(whereClause.getChildCount()==1)
		{
			if(!whereClause.getChild(0).getText().contains("="))
				throw new RuntimeException(new DBAppException("Unsupported/Wrong SQL statement"));
			return;
		}
		if(!whereClause.getChild(1).getText().equalsIgnoreCase("and")) // conditions aren't anded together
			throw new RuntimeException(new DBAppException("Unsupported/Wrong SQL statement"));
		if(!whereClause.getChild(2).getText().contains("=")) // condition uses something other than = (ex: > or <)
			throw new RuntimeException(new DBAppException("Unsupported/Wrong SQL statement"));
		checkOperatorAndConditions(whereClause.getChild(0)); // check the rest of conditions recursively
	}
	private void checkOperator(ParseTree whereClause)
	{
		if(!whereClause.getChild(0).getChild(1).getText().contains("="))
			throw new RuntimeException(new DBAppException("Unsupported/Wrong SQL statement"));
	}
	private void addValuesToDeleteHashTable(Hashtable<String,Object> htblColNameValue, ParseTree expression)
	{
		String[] nameValue = new String[2];
		if(expression.getChildCount()==1)
		{
			nameValue = expression.getChild(0).getText().split("=");
			htblColNameValue.put(nameValue[0], parseValue(nameValue[1]));
			return;
		}
		nameValue = expression.getChild(2).getText().split("=");
		htblColNameValue.put(nameValue[0], parseValue(nameValue[1]));
		addValuesToDeleteHashTable(htblColNameValue, expression.getChild(0));
	}

	public void enterSimpleSelect(MySqlParser.SimpleSelectContext ctx) 
	{
		QuerySpecificationContext queryctx = ctx.querySpecification();
		checkSelectHasOnlyOneTable(ctx.querySpecification().fromClause());
		String tableName = ctx.querySpecification().fromClause().tableSources().tableSource(0).getText();
		ArrayList<SQLTerm> arrListSQLTerms = new ArrayList<SQLTerm>();
		ArrayList<String> strarrListOperators = new ArrayList<String>();
		if(queryctx.fromClause().whereExpr==null || queryctx.groupByClause() !=null || queryctx.groupByClause() !=null 
				|| queryctx.limitClause() != null || queryctx.orderByClause() !=null || queryctx.windowClause() !=null 
				|| !queryctx.getText().contains("*"))
			throw new RuntimeException(new DBAppException("Unsupported/Wrong SQL statement"));
		
		fillOperatorsAndTerms(arrListSQLTerms,strarrListOperators,queryctx.fromClause().whereExpr.getChild(0).getParent(), tableName);




		SQLTerm[] arrSQLTerms = arrListSQLTerms.toArray(SQLTerm[]::new);
		String[] strarrOperators = strarrListOperators.toArray(String[]::new);
		try {
			result = database.selectFromTable(arrSQLTerms, strarrOperators);
		} catch (ClassNotFoundException | DBAppException | IOException e) {
			throw new RuntimeException(e);
		}
	}
	private void fillOperatorsAndTerms(ArrayList<SQLTerm> arrListSQLTerms, ArrayList<String> strarrListOperators, 
			ParseTree whereExpression, String tableName)
	{
		if(whereExpression.getChildCount()!=1)
		{
			fillOperators(strarrListOperators,whereExpression);
			fillSQLterms(arrListSQLTerms, whereExpression, tableName);
		}
		else
		{
			SQLTerm term = new SQLTerm();
			ParseTree condition = whereExpression.getChild(0); 
			term._strTableName = tableName;
			term._strColumnName = condition.getChild(0).getText();
			term._strOperator = condition.getChild(1).getText();
			term._objValue = parseValue(condition.getChild(2).getText());
			
			arrListSQLTerms.add(term);
		}
	}
	private void fillOperators( ArrayList<String> strarrListOperators, ParseTree whereExpression)
	{
		if(whereExpression.getChildCount()==1)
			return;
		String operator = whereExpression.getChild(1).getText().toUpperCase();
		strarrListOperators.add(0, operator);
		fillOperators(strarrListOperators, whereExpression.getChild(0));
	}
	private void fillSQLterms(ArrayList<SQLTerm> arrListSQLTerms, ParseTree whereExpression, String tableName)
	{
		SQLTerm term = new SQLTerm();
		ParseTree condition;
		
		// takes the last condition on the right, and splits it into 3 pieces:
		// column name, operator, and object value
		if(whereExpression.getChildCount()==1)
			condition = whereExpression.getChild(0); 
		else
			condition = whereExpression.getChild(2).getChild(0); 
		term._strTableName = tableName;
		term._strColumnName = condition.getChild(0).getText();
		term._strOperator = condition.getChild(1).getText();
		term._objValue = parseValue(condition.getChild(2).getText());
		
		arrListSQLTerms.add(0, term);
		if(whereExpression.getChildCount()==1)
			return;
		fillSQLterms(arrListSQLTerms, whereExpression.getChild(0), tableName);
	}
	
	private void checkSelectHasOnlyOneTable(FromClauseContext fromctx)
	{
		if(fromctx.tableSources().tableSource().size()>1) // ie select * from x,y where x=y
			throw new RuntimeException(new DBAppException("Unsupported/Wrong SQL statement"));
		// TODO there has to be better way to see if a join is made
		if(fromctx.tableSources().tableSource().get(0).getText().toLowerCase().contains("join")) // ie select* from x join y on x=y
			throw new RuntimeException(new DBAppException("Unsupported/Wrong SQL statement"));

	}

	// turns datatype entered to the one we use in the project
	// ex: integer becomes java.lang.Integer
	private String getDataType(String datatypeName)
	{
		datatypeName = datatypeName.toLowerCase();
		if(datatypeName.contains("int"))
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
			value = value.replaceAll("'", "");
			return new String(value);
		}
		else if(value.contains("."))
			return new Double(Double.parseDouble(value));
		else
			return new Integer(Integer.parseInt(value));
	}

	private String getPrimaryKeyColumn(String tableName)
	{
		try {
			return database.getPrimaryKeyName(DBApp.getColumnData(tableName));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	public void visitErrorNode(ErrorNode node)
	{
		throw new RuntimeException(new DBAppException("Unsupported/Wrong SQL statement"));
	}
}
