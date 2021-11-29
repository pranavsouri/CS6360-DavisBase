package com.utd.davisbasedb.main;

/*
 *  @author Team Rome 
 *  @version 1.0
 *	Helper Function for DavisBase
 */

public class DavisBaseHelper {
	
	public static String line(String s, int num) {
		String a = "";
		for(int i = 0; i < num; i++) {
			a += s;
		}
		return a;
	}
	
	public static void help() {
    	System.out.println(line("*",80));
		System.out.println("SUPPORTED COMMANDS\n");
		System.out.println("All commands below are case insensitive\n");
		System.out.println("SHOW TABLES;");
		System.out.println("\tDisplay the names of all tables.\n");
		System.out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
		System.out.println("\tDisplay table records whose optional <condition>");
		System.out.println("\tis <column_name> = <value>.\n");
		System.out.println("DROP TABLE <table_name>;");
		System.out.println("\tRemove table data (i.e. all records) and its schema.\n");
		System.out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
		System.out.println("\tModify records data whose optional <condition> is\n");
		System.out.println("VERSION;");
		System.out.println("\tDisplay the program version.\n");
		System.out.println("HELP;");
		System.out.println("\tDisplay this help information.\n");
		System.out.println("EXIT;");
		System.out.println("\tExit the program.\n");
		System.out.println(line("*",80));
    }

}
