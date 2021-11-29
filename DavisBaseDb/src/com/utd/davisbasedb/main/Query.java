package com.utd.davisbasedb.main;

import java.io.File;
import java.util.Map;

import com.utd.davisbasedb.utils.DatabaseConstants;

/*
 *  @author Team Rome 
 *  @version 1.0
 *  This class is used for execution of queries
 */

public class Query {

	/*
	 *  Function to parse query create table
	 */
	
	public static void parseCreateTable(String userCommand) {
		String createTokens[] = userCommand.split(" ");
		String[] temp = userCommand.replaceAll("\\(", "").replaceAll("\\)", "").split(createTokens[2]);
		String[] columnNames = temp[1].trim().split(",");
		
		for(int i = 0; i < columnNames.length; i++) {
			columnNames[i] = columnNames[i].trim();
		}
		
		if(isTableExists(createTokens[2])) {
			System.out.println("Table " + createTokens[2] + " already exists");
			return;
		}
		else {
			FileOperations.createTable(createTokens[2].trim(), columnNames);
		}		
	}

	/*
	 *  Function to create Databases
	 */
	
	public static void createDatabase(String dbName) {
		
		try {
			File db = new File ("data\\" + dbName);
			if(db.exists()) {
				System.out.println("Database already exists!");
				return;				
			}
			else {
				db.mkdir();
				System.out.println("Database " + dbName + " created successfully");
			}
		}
		catch (SecurityException se) {
			System.out.println("Unable to create metadata directory :"+se);	
		}		
	}

	/*
	 *  Function to showTables
	 */
	
	public static void showTables(String userCommand) {
		String[] columnNames = {"*"};
		String[] condition = new String[0];
		FileOperations.selectFromTable("davisbase_tables", columnNames, condition);
	}

	/*
	 *  Function to showDatabases
	 */
	
	public static void showDatabases() {

		File file = new File("data");
		String[] listDir = file.list();
		for (String db : listDir) {
			if (db.equals("meta_data"))
				continue;
			System.out.println(db);
		}
	}
		
	
	/*
	 *  Function to check if DataBase exists
	 */
	
	public static boolean isDataBaseExists(String dbName) {
		File db = new File("data\\" + dbName);
		if(db.exists()) {
			DavisBasePrompt.defaultDatabase = dbName;
			return true;
		}
		return false;
	}
	
	/*
	 *  Function to check if Table exists
	 */
	
	public static boolean isTableExists(String tableName) {
		tableName = tableName+".tbl"; 
		File table;
		try {
			if (tableName.equalsIgnoreCase("davisbase_tables.tbl") || tableName.equalsIgnoreCase("davisbase_columns.tbl")) {
				table = new File("data\\meta_data" );
			} 
			else {
				table = new File("data\\" + DavisBasePrompt.defaultDatabase);
			}
			if(!table.exists()) {
				System.out.println("Database doesn't exist! Creating database");
				table.mkdir();
			}
			else {
				String[] listTable = table.list();
				for(String tbl : listTable) {
					if(tbl.equals(tableName)) {
						return true;
					}
				}
			}
		}
		catch (SecurityException se) {
			System.out.println("Unable to create metadata directory :"+se);	
		}		
		return false;
	}


	/*
	 *  Function to parse query update table
	 */
	
	public static void parseUpdate(String userCommand) {
		String[] updateTokens = userCommand.split("set");
		String[] table = updateTokens[0].split(" ");
		String tableName = table[1].trim();
		if(!isTableExists(tableName)) {
			System.out.println("Table " + tableName + " doesn't exist");
			return;
		}
		else {
			if (updateTokens[1].contains("where")) {
				String[] updateCondition = updateTokens[1].trim().split("where");
				String setValue = updateCondition[0].trim();
				String whereClause = updateCondition[1].trim();
				String[] retrieveSetValue = parseCondition(setValue);
				String[] retrieveWhereClause = parseCondition(whereClause);
				FileOperations.updateTable(tableName, retrieveSetValue, retrieveWhereClause);
			}
			else {
				String setValue = updateTokens[1].trim();
				String[] retrieveSetValue = parseCondition(setValue);
				String[] retrieveWhereClause = new String[0];
				FileOperations.updateTable(tableName, retrieveSetValue, retrieveWhereClause);
			}
			System.out.println("Table updated successfully");
		}		
	}

	/*
	 *  Function to parse query delete table
	 */
	
	public static void parseDelete(String userCommand) {
		String[] deleteTokens = userCommand.split("where");
		String[] table = deleteTokens[0].split(" ");
		String tableName = table[1].trim();
		if(!isTableExists(tableName)) {
			System.out.println("Table" + tableName + "doesn't exist");
			return;
		}
		else {
			String[] retrieveWhereCondition = parseCondition(deleteTokens[0].trim());
			FileOperations.deleteFromTable(tableName, retrieveWhereCondition);			
		}		
	}
	
	/*
	 *  Function to parse where Condition and Set values
	 */
	
	public static String[] parseCondition(String clause) {

		String condition[] = new String[3];
		String values[] = new String[2];
		if (clause.contains("=")) {
			values = clause.split("=");
			condition[0] = values[0].trim();
			condition[1] = "=";
			condition[2] = values[1].trim();
		}

		else if (clause.contains(">")) {
			values = clause.split(">");
			condition[0] = values[0].trim();
			condition[1] = ">";
			condition[2] = values[1].trim();
		}

		else if (clause.contains("<")) {
			values = clause.split("<");
			condition[0] = values[0].trim();
			condition[1] = "<";
			condition[2] = values[1].trim();
		}

		else if (clause.contains(">=")) {
			values = clause.split(">=");
			condition[0] = values[0].trim();
			condition[1] = ">=";
			condition[2] = values[1].trim();
		}

		else if (clause.contains("<=")) {
			values = clause.split("<=");
			condition[0] = values[0].trim();
			condition[1] = "<=";
			condition[2] = values[1].trim();
		}

		else if (clause.contains("!=")) {
			values = clause.split("!=");
			condition[0] = values[0].trim();
			condition[1] = "!=";
			condition[2] = values[1].trim();
		}
		return condition;
	}

	/*
	 *  Function to parse drop table
	 */
	
	public static void dropTable(String tableName) {
		FileOperations.dropTable(tableName);
		File file = new File("data/"+DatabaseConstants.DEFAULTDB+"/"+tableName+".tbl");
		if(file.delete()) {
			System.out.println("Deleted the file: " + file.getName());
		}
		else {
			System.out.println("Failed to delete the table file");
		}
	}

	/*
	 *  Function to parse insert table
	 */
	
	public static void parseInsert(String userCommand) {
		String insertTokens[] = userCommand.split(" ");
		String[] temp = userCommand.replaceAll("\\(", "").replaceAll("\\)", "").split("values");
		String[] values = temp[1].trim().split(",");
		String tableName = insertTokens[2].trim();
		
		Map<Integer, String> columnNamesMap = FileOperations.getColumnsFromDavisbaseColumns(tableName);
		int valueCount = 0;
		String[] values1 = new String[columnNamesMap.size()];
		for(valueCount = 0; valueCount < values.length; valueCount++) {
			values1[valueCount] = values[valueCount].trim();
		}
		/* Insert null where values is not given by the user */
		if(values.length < columnNamesMap.size()) {
			for(int i = valueCount; i <= columnNamesMap.size() - valueCount; i++) {
				values1[i] = "null";
			}
		}
		if(!isTableExists(tableName)) {
			System.out.println("Table" +  tableName + "doesn't exists");
			return;
		}
		else {
			FileOperations.insertIntoTable(tableName, values1);
		}		
	}

	/*
	 *  Function to parse select query
	 */
	
	public static void parseSelect(String userCommand) {
		String[] selectTokens = userCommand.split("where");
		String tableName = selectTokens[0].split("from")[1].trim();
		String[] columnNames = selectTokens[0].split("from")[0].replaceAll("select", " ").split(",");
		String[] selectCondition = new String[0];

		if (!isTableExists(tableName)) {
			System.out.println("Table " + tableName + " doesn't exist");
			return;
		} 
		else {
			for (int colCount = 0; colCount < columnNames.length; colCount++) {
				columnNames[colCount] = columnNames[colCount].trim();
			}

			if (selectTokens.length > 1) {
				selectCondition = parseCondition(selectTokens[1]);
			}
			FileOperations.selectFromTable(tableName, columnNames, selectCondition);
		}
	}
}
