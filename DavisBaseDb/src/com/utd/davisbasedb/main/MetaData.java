package com.utd.davisbasedb.main;

import java.io.File;

import com.utd.davisbasedb.utils.DatabaseConstants;

/*
 *  @author Team Rome 
 *  @version 1.0
 *	Function for initializing database meta-data.
 */

public class MetaData {
	
	public static void initializeMetaData() 
	{
		try {
			File data = new File(DatabaseConstants.DEFAULT_DATA_DIRNAME);
			if (!data.exists()) 
			{
				data.mkdir();
			}
			File metaData = new File("data\\meta_data");
			if (metaData.mkdir()) 
			{
				System.out.println("System directory 'data\\meta_data' doesn't exit, Initializing metadata!");
				FileOperations.initializeDataStore();
			} 
			else 
			{
				boolean catalog = false;
				String meta_columns = DatabaseConstants.SYSTEM_TABLES_TABLENAME;
				String meta_tables = DatabaseConstants.SYSTEM_COLUMNS_TABLENAME;
				String[] tableList = metaData.list();

				for (int i = 0; i < tableList.length; i++) 
				{
					if (tableList[i].equals(meta_columns))
						catalog = true;
				}
				if (!catalog) 
				{
					System.out.println(
							"System table 'davisbase_columns.tbl' does not exit, initializing davisbase_columns");
					System.out.println();
					FileOperations.initializeDataStore();
				}
				catalog = false;
				for (int i = 0; i < tableList.length; i++) 
				{
					if (tableList[i].equals(meta_tables))
						catalog = true;
				}
				if (!catalog) 
				{
					System.out.println(
							"System table 'davisbase_tables.tbl' does not exit, initializing davisbase_tables");
					System.out.println();
					FileOperations.initializeDataStore();
				}
			}
		} 
		catch (SecurityException se) 
		{
			System.out.println("Meta Data files not created " + se);
		}
	}

}
