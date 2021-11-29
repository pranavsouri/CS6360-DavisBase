package com.utd.davisbasedb.main;

import com.utd.davisbasedb.utils.DatabaseConstants;

/*
 *  @author Team Rome 
 *  @version 1.0
 *	Function for parsing User queries based on token values.
 */

public class Parser {
	
public static void parseUserQueries(String userQuery) {
		
		String[] tokens = userQuery.split(" ");
		switch(tokens[0]) {
			case "use":
				if(tokens.length == 1 || tokens[1].equals("")) {
					System.out.println("Incorrect input. Refer to the help section");	
				}
			    else if(!Query.isDataBaseExists(tokens[1])) {
					System.out.println("Database doesn't exist");
				}
				else {
					System.out.println("using "+ DatabaseConstants.DEFAULTDB);
				}
				break;
			case "create":
				if (tokens.length == 1) {
					System.out.println("Incorrect input. Refer to the help section");
				}
				else if(tokens[1].equals("table")) {
					Query.parseCreateTable(userQuery);
				}
				else if(tokens[1].equals("database")) {
					Query.createDatabase(tokens[2]);
				}
				else {
					System.out.println("Incorrect input. Refer to the help section");				
				}
				break;
			case "show":
				if (tokens.length == 1) {
					System.out.println("Incorrect input. Refer to the help section");
				}
				else if(tokens[1].equals("tables")) {
					Query.showTables(userQuery);
				}
				else if(tokens[1].equals("databases")) {
					Query.showDatabases();
				}
				else {
					System.out.println("Incorrect input. Refer to the help section");				
				}
				break;
			case "select":
				if (tokens.length < 4) {
					System.out.println("Incorrect input. Refer to the help section");
				}
				else {
					Query.parseSelect(userQuery);
				}
				break;
			case "drop":
				if(tokens[1].equals("table")) {
					String tableName = tokens[2].trim();
					if(!Query.isTableExists(tableName)) {
						System.out.println("Table " + tableName + " doesn't exist");
						break;
					}
					else {
						Query.dropTable(tableName);
					}
				}		
				break;
			case "insert":
				Query.parseInsert(userQuery);
				break;
			case "delete":
				Query.parseDelete(userQuery);
				break;
			case "update":
				Query.parseUpdate(userQuery);
				break;
			case "version":
				System.out.println();
				System.out.println("DavisBaseLite Version " + DatabaseConstants.VERSION);
				break;
			case "help":
				DavisBaseHelper.help();
				break;
			case "exit":
				System.out.println();
				break;
			default:
				System.out.println("Wrong user command " + userQuery);
				break;		
		}		
	
	}
}
