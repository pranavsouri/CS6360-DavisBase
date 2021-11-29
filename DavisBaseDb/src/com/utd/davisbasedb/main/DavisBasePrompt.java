package com.utd.davisbasedb.main;

import java.util.Scanner;

import com.utd.davisbasedb.utils.DatabaseConstants;


/*
 *  @author Team Rome 
 *  @version 1.0
 *	Functions for initializing the database, creating prompt and taking user input.
 */

public class DavisBasePrompt {
	
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	static String defaultDatabase = DatabaseConstants.DEFAULTDB;
	
    public static void main(String[] args) {
    	
		// Display the welcome screen
		splashScreen();
		// Variable to collect user input from the prompt
		String userQueries = ""; 
		// Initialize the meta data
		MetaData.initializeMetaData();
		while(!userQueries.equals("exit")) {
			System.out.print(DatabaseConstants.PROMPT);
			userQueries = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			Parser.parseUserQueries(userQueries);
		}
		System.out.println("Exiting from DavisBase");
	}

    // Display Splash screen
	public static void splashScreen() {
		System.out.println(line("-",80));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println("DavisBaseLite Version " + DatabaseConstants.VERSION);
		System.out.println(DatabaseConstants.COPYRIGHT);
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-",80));
	}
	
	 /* @param s The String to be repeated
	 *  @param num The number of time to repeat String s.
	 *  @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s, int num) {
		String a = "";
		for(int i = 0; i < num; i++) {
			a += s;
		}
		return a;
	}
	
}
