DavisBaseDb :

Prerequisites: 


1) Eclipse IDE should be installed. I have used Eclipse IDE to make my application.
2) Java version 9 should be installed.

Steps to compile and build and install :

The DavisBaseDb folder should be exported in the eclipse workspace.

2) The folder contains all the dependencies which is needed to compile the application.

3) To run the application :
Right click on DavisBaseDb-> Run as -> 2 Java Application.

Commands supported :

1) create table items (row_id INT PRIMARY KEY, price INT NOT NULL);
 
Assumption for this command : row_id is the primary key and must be the first column of any table to be created. 

2) insert into items values (1, 1000);

3) select * from items;
 row_id | price | 
 1   |   1000
 2   |   2000

 4) update items set price = 3000 where row_id = 2;
Table updated successfully

Assumption : where clause in update should be row_id

5) help;


********************************************************************************
SUPPORTED COMMANDS

All commands below are case insensitive

SHOW TABLES;
	Display the names of all tables.

SELECT <column_list> FROM <table_name> [WHERE <condition>];
	Display table records whose optional <condition>
	is <column_name> = <value>.

DROP TABLE <table_name>;
	Remove table data (i.e. all records) and its schema.

UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];
	Modify records data whose optional <condition> is

VERSION;
	Display the program version.

HELP;
	Display this help information.

EXIT;
	Exit the program.

********************************************************************************

6) show tables;
 rowid | table_name | 
 1   |   davisbase_tables
 2   |   davisbase_columns
 3   |   items

 7) show databases;
user_data

8) version;

DavisBaseLite Version v1.0

9) select * from davisbase_tables;
 rowid | table_name | 
 1   |   davisbase_tables
 2   |   davisbase_columns
 3   |   items

 10) select * from davisbase_columns;
 rowid | table_name | column_name | data_type | ordinal_position | is_nullable | 
 1   |   davisbase_tables   |   rowid   |   INT   |   1   |   NO
 2   |   davisbase_tables   |   table_name   |   TEXT   |   2   |   NO
 3   |   davisbase_columns   |   rowid   |   INT   |   1   |   NO
 4   |   davisbase_columns   |   table_name   |   TEXT   |   2   |   NO
 5   |   davisbase_columns   |   column_name   |   TEXT   |   3   |   NO
 6   |   davisbase_columns   |   data_type   |   TEXT   |   4   |   NO
 7   |   davisbase_columns   |   ordinal_position   |   TINYINT   |   5   |   NO
 8   |   davisbase_columns   |   is_nullable   |   TEXT   |   6   |   NO
 9   |   items   |   row_id   |   INT   |   1   |   NO
 10   |   items   |   price   |   INT   |   2   |   NO

 11) drop table items;

 12) use user_data;
using user_data

13) create database employee;
Database employee created successfully

14) exit;
Exiting from DavisBase
