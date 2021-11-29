package com.utd.davisbasedb.utils;

public interface DatabaseConstants {

	String PROMPT = "romeSQL> ";
	String VERSION = "v1.0";
	String COPYRIGHT = "©2021 Team Rome";
	
	String DEFAULTDB = "user_data";
	String DEFAULT_FILE_EXTENSION = ".tbl";
	String DEFAULT_DATA_DIRNAME = "data";
	String DEFAULT_CATALOG_DATABASENAME = "meta_data";
	String SYSTEM_TABLES_TABLENAME = "davisbase_tables.tbl";
	String SYSTEM_COLUMNS_TABLENAME = "davisbase_columns.tbl";
	int PAGE_SIZE = 512;
	int PAYLOAD_HEADER_SIZE = 6;
	int DEFAULT_NO_OF_SYSTEM_TABLES = 0x02;
	String DATEPATTERN = "yyyy-MM-dd_HH:mm:ss";

}
