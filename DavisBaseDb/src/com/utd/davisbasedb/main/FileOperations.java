package com.utd.davisbasedb.main;

import java.io.File;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import com.utd.davisbasedb.utils.DatabaseConstants;

/*
 *  @author Team Rome 
 *  @version 1.0
 *  This class is used to perform file operations
 */

public class FileOperations {

	/*
	 * This static method creates the DavisBase data storage container and then
	 * initializes two .tbl files to implement the two system tables,
	 * davisbase_tables and davisbase_columns
	 */

	public static void initializeDataStore() {

		/* Create data directory at the current OS location to hold */
		try {
			File dataDir = new File("data\\meta_data");
			String[] oldTableFiles;
			oldTableFiles = dataDir.list();
			for (int i = 0; i < oldTableFiles.length; i++) {
				File anOldFile = new File(dataDir, oldTableFiles[i]);
				anOldFile.delete();
			}
		} catch (SecurityException se) {
			System.out.println("Unable to create data container directory");
			System.out.println(se);
		}
		createDavisbaseTables();
		createDavisbaseColumns();
	}

	/*
	 * Create davisbase_tables system catalog
	 */

	public static void createDavisbaseTables() {
		int recordHeaderSize = 6;
		try {
			RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data\\meta_data\\davisbase_tables.tbl",
					"rw");
			/* Initially, the file is one page in length */
			davisbaseTablesCatalog.setLength(DatabaseConstants.PAGE_SIZE);
			/* Set file pointer to the beginnning of the file */
			davisbaseTablesCatalog.seek(0);
			/* Write 0x0D to the page header to indicate that it's a leaf page. */
			davisbaseTablesCatalog.writeByte(0x0D);
			davisbaseTablesCatalog.writeByte(DatabaseConstants.DEFAULT_NO_OF_SYSTEM_TABLES);
			int[] offset = new int[2];
			int sizeofRecord1 = 18 + recordHeaderSize;
			int sizeofRecord2 = 19 + recordHeaderSize;
			offset[0] = (int) (DatabaseConstants.PAGE_SIZE - sizeofRecord1);
			offset[1] = offset[0] - sizeofRecord2;
			/* Header */
			davisbaseTablesCatalog.writeShort(offset[1]); // 2 bytes length of the record
			davisbaseTablesCatalog.writeInt(-1);
			davisbaseTablesCatalog.writeShort(offset[0]); // Address of 1st record
			davisbaseTablesCatalog.writeShort(offset[1]); // Address of 2nd record
			/* Write record 0 */
			davisbaseTablesCatalog.seek(offset[0]);
			davisbaseTablesCatalog.writeShort(18); // Length of payload
			davisbaseTablesCatalog.writeInt(1); // row id
			davisbaseTablesCatalog.writeByte(1); // 1 column
			davisbaseTablesCatalog.writeByte(28); // length of payload TEXT + 0x0C
			davisbaseTablesCatalog.writeBytes("davisbase_tables");
			/* Write record 1 */
			davisbaseTablesCatalog.seek(offset[1]);
			davisbaseTablesCatalog.writeShort(19); // Length of payload
			davisbaseTablesCatalog.writeInt(2); // row id
			davisbaseTablesCatalog.writeByte(1); // 1 column
			davisbaseTablesCatalog.writeByte(29); // length of payload + 0x0c
			davisbaseTablesCatalog.writeBytes("davisbase_columns");
			davisbaseTablesCatalog.close();
		} catch (Exception e) {
			System.out.println("Unable to create the database_tables file");
			System.out.println(e);
		}
	}

	/*
	 * Create davisbase_columns system catalog
	 */

	/*
	 * Create davisbase_columns system catalog
	 */

	public static void createDavisbaseColumns() {

		int recordHeaderSize = 6;
		/* Create davisbase_columns systems catalog */
		try {
			RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile("data\\meta_data\\davisbase_columns.tbl", "rw");
			/* Initially the file is one page in length */
			davisbaseColumnsCatalog.setLength(DatabaseConstants.PAGE_SIZE);
			/* Set file pointer to the beginning of the file */
			davisbaseColumnsCatalog.seek(0);
			/* Write 0x0D to the page header to indicate a leaf page. */
			davisbaseColumnsCatalog.write(0x0D);
			davisbaseColumnsCatalog.write(0x08); // No. of records

			int recordSize[] = new int[] { 33, 39, 34, 40, 41, 39, 49, 41 };
			int offset[] = new int[8];

			offset[0] = (int) (DatabaseConstants.PAGE_SIZE - (recordSize[0] + recordHeaderSize));

			for (int i = 1; i < offset.length; i++) {
				offset[i] = offset[i - 1] - (recordSize[i] + recordHeaderSize);
			}

			davisbaseColumnsCatalog.writeShort(offset[7]);
			davisbaseColumnsCatalog.writeInt(-1);

			for (int i = 0; i < offset.length; i++) {
				davisbaseColumnsCatalog.writeShort(offset[i]);
			}

			/* Record 1 */
			davisbaseColumnsCatalog.seek(offset[0]);
			/* Record Header */
			davisbaseColumnsCatalog.writeShort(recordSize[0]);
			davisbaseColumnsCatalog.writeInt(1); // rowid
			/* Payload header */
			davisbaseColumnsCatalog.writeByte(5); // No. of columns
			/* DataType of columns */
			davisbaseColumnsCatalog.writeByte(28);
			davisbaseColumnsCatalog.writeByte(17);
			davisbaseColumnsCatalog.writeByte(15);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			/* Payload Content */
			davisbaseColumnsCatalog.writeBytes("davisbase_tables");
			davisbaseColumnsCatalog.writeBytes("rowid");
			davisbaseColumnsCatalog.writeBytes("INT");
			davisbaseColumnsCatalog.writeByte(1);
			davisbaseColumnsCatalog.writeBytes("NO");

			/* Record 2 */
			davisbaseColumnsCatalog.seek(offset[1]);
			/* Record Header */
			davisbaseColumnsCatalog.writeShort(recordSize[1]);
			davisbaseColumnsCatalog.writeInt(2); // rowid
			/* Payload header */
			davisbaseColumnsCatalog.writeByte(5); // No. of columns
			/* DataType of columns */
			davisbaseColumnsCatalog.writeByte(28);
			davisbaseColumnsCatalog.writeByte(22);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			/* Payload Content */
			davisbaseColumnsCatalog.writeBytes("davisbase_tables");
			davisbaseColumnsCatalog.writeBytes("table_name");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(2);
			davisbaseColumnsCatalog.writeBytes("NO");

			/* Record 3 */
			davisbaseColumnsCatalog.seek(offset[2]);
			/* Record Header */
			davisbaseColumnsCatalog.writeShort(recordSize[2]);
			davisbaseColumnsCatalog.writeInt(3); // rowid
			/* Payload header */
			davisbaseColumnsCatalog.writeByte(5); // No. of columns
			/* DataType of columns */
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(17);
			davisbaseColumnsCatalog.writeByte(15);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			/* Payload Content */
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("rowid");
			davisbaseColumnsCatalog.writeBytes("INT");
			davisbaseColumnsCatalog.writeByte(1);
			davisbaseColumnsCatalog.writeBytes("NO");

			/* Record 4 */
			davisbaseColumnsCatalog.seek(offset[3]);
			/* Record Header */
			davisbaseColumnsCatalog.writeShort(recordSize[3]);
			davisbaseColumnsCatalog.writeInt(4); // rowid
			/* Payload header */
			davisbaseColumnsCatalog.writeByte(5); // No. of columns
			/* DataType of columns */
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(22);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			/* Payload Content */
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("table_name");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(2);
			davisbaseColumnsCatalog.writeBytes("NO");

			/* Record 5 */
			davisbaseColumnsCatalog.seek(offset[4]);
			/* Record Header */
			davisbaseColumnsCatalog.writeShort(recordSize[4]);
			davisbaseColumnsCatalog.writeInt(5); // rowid
			/* Payload header */
			davisbaseColumnsCatalog.writeByte(5); // No. of columns
			/* DataType of columns */
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(23);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			/* Payload Content */
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("column_name");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(3);
			davisbaseColumnsCatalog.writeBytes("NO");

			/* Record 6 */
			davisbaseColumnsCatalog.seek(offset[5]);
			/* Record Header */
			davisbaseColumnsCatalog.writeShort(recordSize[5]);
			davisbaseColumnsCatalog.writeInt(6); // rowid
			/* Payload header */
			davisbaseColumnsCatalog.writeByte(5); // No. of columns
			/* DataType of columns */
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(21);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			/* Payload Content */
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("data_type");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeBytes("NO");

			/* Record 7 */
			davisbaseColumnsCatalog.seek(offset[6]);
			/* Record Header */
			davisbaseColumnsCatalog.writeShort(recordSize[6]);
			davisbaseColumnsCatalog.writeInt(7); // rowid
			/* Payload header */
			davisbaseColumnsCatalog.writeByte(5); // No. of columns
			/* DataType of columns */
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(28);
			davisbaseColumnsCatalog.writeByte(19);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			/* Payload Content */
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("ordinal_position");
			davisbaseColumnsCatalog.writeBytes("TINYINT");
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeBytes("NO");

			/* Record 8 */
			davisbaseColumnsCatalog.seek(offset[7]);
			/* Record Header */
			davisbaseColumnsCatalog.writeShort(recordSize[7]);
			davisbaseColumnsCatalog.writeInt(8); // rowid
			/* Payload header */
			davisbaseColumnsCatalog.writeByte(5); // No. of columns
			/* DataType of columns */
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(23);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			/* Payload Content */
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("is_nullable");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(6);
			davisbaseColumnsCatalog.writeBytes("NO");

			davisbaseColumnsCatalog.close();
		} catch (Exception e) {
			System.out.println("Unable to create the database_columns file");
			System.out.println(e);
		}
	}

	/*
	 * Function to store Create table query in file
	 */

	public static void createTable(String tableName, String[] columnNames) {
		try {
			RandomAccessFile tableFile = new RandomAccessFile(
					"data\\" + DavisBasePrompt.defaultDatabase + "\\" + tableName + ".tbl", "rw");
			tableFile.setLength(DatabaseConstants.PAGE_SIZE);
			tableFile.seek(0);
			tableFile.writeByte(0x0D);
			tableFile.seek(2);
			tableFile.writeShort(DatabaseConstants.PAGE_SIZE);
			tableFile.writeInt(-1);
			tableFile.close();

			/* Update metadata davisbase_tables.tbl */
			RandomAccessFile davisbaseTables = new RandomAccessFile("data/meta_data/davisbase_tables.tbl", "rw");
			int noOfPages = (int) (davisbaseTables.length() / DatabaseConstants.PAGE_SIZE);
			int pageNo = 0;

			/* Stores the records in the order they are inserted */

			Set<Record> recordSet = new LinkedHashSet<Record>();

			for (int i = 0; i < noOfPages; i++) {
				davisbaseTables.seek((i * DatabaseConstants.PAGE_SIZE) + 4);
				/* To check whether it is the rightmost leaf page */
				if (davisbaseTables.readInt() == -1) {
					pageNo = i;
					davisbaseTables.seek((i * DatabaseConstants.PAGE_SIZE) + 1);
					int noOfRecords = davisbaseTables.readByte();
					short[] recordOffset = new short[noOfRecords];
					davisbaseTables.seek((i * DatabaseConstants.PAGE_SIZE) + 8);
					for (int offset = 0; offset < noOfRecords; offset++) {
						recordOffset[offset] = davisbaseTables.readShort();
					}
					recordSet = getRecords(davisbaseTables, recordOffset, i);
				}
			}
			davisbaseTables.close();
			List<Record> recordList = new ArrayList<Record>(recordSet);
			Collections.sort(recordList, new RowIdComparator());
			Record lastRecord = recordList.get(recordList.size() - 1);
			int key = lastRecord.getRowId() + 1;

			String[] values = { Integer.toString(key), tableName };
			insertIntoTable("davisbase_tables", values);

			/* Update metadata davisbase_tables.tbl */
			RandomAccessFile davisbaseColumns = new RandomAccessFile("data/meta_data/davisbase_columns.tbl", "rw");
			noOfPages = (int) (davisbaseColumns.length() / DatabaseConstants.PAGE_SIZE);
			pageNo = 0;

			/* Stores the records in the order they are inserted */

			recordSet = new LinkedHashSet<Record>();

			for (int i = 0; i < noOfPages; i++) {
				davisbaseColumns.seek((i * DatabaseConstants.PAGE_SIZE) + 4);
				/* To check whether it is the rightmost leaf page */
				if (davisbaseColumns.readInt() == -1) {
					pageNo = i;
					davisbaseColumns.seek((i * DatabaseConstants.PAGE_SIZE) + 1);
					int noOfRecords = davisbaseColumns.readByte();
					short[] recordOffset = new short[noOfRecords];
					davisbaseColumns.seek((i * DatabaseConstants.PAGE_SIZE) + 8);
					for (int offset = 0; offset < noOfRecords; offset++) {
						recordOffset[offset] = davisbaseColumns.readShort();
					}
					recordSet = getRecords(davisbaseColumns, recordOffset, i);			
				}
			}
			davisbaseColumns.close();

			recordList = new ArrayList<Record>(recordSet);
			Collections.sort(recordList, new RowIdComparator());
			lastRecord = recordList.get(recordList.size() - 1);
			key = lastRecord.getRowId();

			for (int i = 0; i < columnNames.length; i++) {
				key = key + 1;
				String[] tempColumnToken = columnNames[i].split(" ");
				String isNull = "YES";

				/*
				 * If length will be greater than 2, it will be either primary key or not null
				 * constraint
				 */
				if (tempColumnToken.length > 2) {
					isNull = "NO";
				}
				String colName = tempColumnToken[0].trim();
				String dataType = tempColumnToken[1].toUpperCase();
				String ordinalPosition = Integer.toString(i + 1);
				String[] val = { Integer.toString(key), tableName, colName, dataType, ordinalPosition, isNull };
				
				insertIntoTable("davisbase_columns", val);
			}
		} catch (Exception e) {
			System.out.println("Error at creating table");
			e.printStackTrace();
		}
	}

	public static void deleteFromTable(String tableName, String[] retrieveWhereCondition) {
		// TODO Auto-generated method stub

	}

	/*
	 * Function to insert values into the table
	 */

	public static void insertIntoTable(String tableName, String[] values) {

		try {
			String filePath = "";
			if (tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns")) {
				filePath = "data/meta_data/" + tableName + ".tbl";
			} else {
				filePath = "data/" + DatabaseConstants.DEFAULTDB + "/" + tableName + ".tbl";
			}

			RandomAccessFile fileTable = new RandomAccessFile(filePath, "rw");
			String condition[] = { "table_name", "=", tableName };
			String columnNames[] = { "*" };
			Set<Record> columnsMeta = getColumnsMeta(columnNames, condition);
			String[] dataType = getDataType(columnsMeta);
			String[] isNull = getIsNull(columnsMeta);
			/* Validation of not null values */
			for (int i = 0; i < values.length; i++) {
				if (values[i].equalsIgnoreCase("null") && isNull[i].equalsIgnoreCase("NO")) {
					System.out.println("Insertion failed: NULL values in not null column");
					return;
				}
			}

			condition = new String[0];

			/* Check for duplicate values for primary key */
			Set<Record> dataSet = getDataValues(tableName, columnNames, condition, filePath);
			for (Record r : dataSet) {
				if (r.getRowId() == Integer.parseInt(values[0])) {
					System.out.println("Duplicate values not allowed for Primary key");
					return;
				}
			}

			/* Get page no where table exists */
			int pageNo = getPageNo(tableName, Integer.parseInt(values[0]), filePath);
			/* Calculate payload size */
			byte[] payLoadDataType = new byte[dataType.length - 1];
			int payLoadSize = 1 + DatabaseConstants.PAYLOAD_HEADER_SIZE + dataType.length - 1;
			for (int i = 0; i < values.length - 1; i++) {
				payLoadDataType[i] = getSerialCode(values[i + 1], dataType[i + 1]);
				payLoadSize = payLoadSize + getDataLength(payLoadDataType[i]);
			}
			

			int recordLocation = getRecordOffset(fileTable, pageNo, payLoadSize);
			if (recordLocation == 0) {
				splitLeafPage(fileTable, pageNo);
				pageNo = getPageNo(tableName, Integer.parseInt(values[0]), filePath);
				recordLocation = getRecordOffset(fileTable, pageNo, payLoadSize);
			}
			Record newRecord = insertRecord(pageNo, Integer.parseInt(values[0]), (short) payLoadSize, payLoadDataType, values);
			writePayLoadToFile(fileTable, newRecord, recordLocation);
		} catch (Exception e) {
			System.out.println("Error at inserting table");
			e.printStackTrace();
		}
	}

	/*
	 * Function to split leaf page on overflow
	 */

	private static void splitLeafPage(RandomAccessFile fileTable, int currentPageNo) {
		int newLeafPage = -1;
		try {
			newLeafPage = (int) fileTable.length() / DatabaseConstants.PAGE_SIZE;
			newLeafPage = newLeafPage + 1;
			fileTable.setLength(newLeafPage * DatabaseConstants.PAGE_SIZE);
			fileTable.seek((newLeafPage - 1) * DatabaseConstants.PAGE_SIZE);
			fileTable.writeByte(0x0D);
		} catch (Exception e) {
			e.printStackTrace();
		}
		int key = splitKeyLocation(fileTable, currentPageNo);
		moveRecordsToNewPage(fileTable, currentPageNo, newLeafPage, key);
	}

	/*
	 * Function to move records to new page
	 */

	private static void moveRecordsToNewPage(RandomAccessFile fileTable, int currentPage, int newLeafPage, int key) {

		try {
			fileTable.seek(currentPage * DatabaseConstants.PAGE_SIZE);
			byte pageType = fileTable.readByte();
			byte noOfRecords = fileTable.readByte();
			
			int mid = (int) Math.ceil(noOfRecords / 2);
			int lowRec = mid - 1;
			int highRec = noOfRecords - lowRec;
			int recordNewOffset = DatabaseConstants.PAGE_SIZE;
			short offset = -1;
			for (int i = mid; i <= noOfRecords; i++) {
				/* Read data from current page */
				fileTable.seek(currentPage * DatabaseConstants.PAGE_SIZE + 8 + (2 * i) - 2);
				offset = fileTable.readShort();
				fileTable.seek(offset);
				int existingRecordSize = fileTable.readShort();		
				int recordSize = existingRecordSize + DatabaseConstants.PAYLOAD_HEADER_SIZE;
				recordNewOffset = recordNewOffset - recordSize;
				fileTable.seek(offset);
				byte[] recordContent = new byte[recordSize];
				fileTable.read(recordContent);

				/* write data to the new page */
				fileTable.seek((newLeafPage - 1) * DatabaseConstants.PAGE_SIZE + 8 + (i - mid) * 2);
				fileTable.writeShort((newLeafPage-1)* DatabaseConstants.PAGE_SIZE + recordNewOffset);
				fileTable.seek((newLeafPage - 1) * DatabaseConstants.PAGE_SIZE + recordNewOffset);
				fileTable.write(recordContent);
			}

			/* Update Current Page */
			fileTable.seek(currentPage * DatabaseConstants.PAGE_SIZE + 8 + (2 * lowRec));
			offset = fileTable.readShort();
			fileTable.seek(currentPage * DatabaseConstants.PAGE_SIZE + 2);
			fileTable.write(offset);

			/* New Page updation */
			fileTable.seek((newLeafPage - 1) * DatabaseConstants.PAGE_SIZE + 2);
			fileTable.writeShort((newLeafPage - 1) * DatabaseConstants.PAGE_SIZE + recordNewOffset);

			/* Update right most page */
			fileTable.seek(currentPage * DatabaseConstants.PAGE_SIZE + 4);
			int rightmostPage = fileTable.readInt();
			fileTable.seek(currentPage * DatabaseConstants.PAGE_SIZE + 4);
			fileTable.writeInt(newLeafPage);
			fileTable.seek((newLeafPage - 1) * DatabaseConstants.PAGE_SIZE + 4);
			fileTable.writeInt(rightmostPage);

			/* Update No of Records */
			fileTable.seek(currentPage * DatabaseConstants.PAGE_SIZE + 1);
			fileTable.write((byte) lowRec);
			fileTable.seek((newLeafPage - 1) * DatabaseConstants.PAGE_SIZE + 1);
			fileTable.write((byte) highRec);
			/* Update Parent */
			int parent = getParent(fileTable, currentPage + 1);
			if(parent == 0) {
				parent = createInteriorPage(fileTable);
				setParent(fileTable, parent, currentPage, key);
				fileTable.seek((parent - 1) * DatabaseConstants.PAGE_SIZE + 4);
				fileTable.writeInt(newLeafPage);
			}
			else {
				fileTable.seek((parent - 1) * DatabaseConstants.PAGE_SIZE + 4);
				if(fileTable.readInt() == currentPage + 1) {
					setParent(fileTable, parent, currentPage, key);
					fileTable.seek((parent - 1) * DatabaseConstants.PAGE_SIZE + 4);
					fileTable.writeInt(newLeafPage);
				}
				else {
					setParent(fileTable, parent, currentPage, key);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}



	/*
	 * Function to populate new parent page
	 */

	private static void setParent(RandomAccessFile fileTable, int parent, int child, int key) {
		try {
			fileTable.seek((parent - 1) * DatabaseConstants.PAGE_SIZE + 1);
			int noOfRecords = fileTable.readByte();
			int cellStartOffset = fileTable.readShort();
			int usedSpace = 8 + noOfRecords * 2 + cellStartOffset;
			short offset = -1;
			/* Size of interior page Record is 8 bytes */
			if ((DatabaseConstants.PAGE_SIZE - usedSpace) >= 8) {
				short recordOffset = (short) ((parent) * DatabaseConstants.PAGE_SIZE);
				TreeMap<Integer, Short> recordMap = new TreeMap<Integer, Short>();
				if (noOfRecords == 0) {
					fileTable.seek((parent - 1) * DatabaseConstants.PAGE_SIZE + 1);
					fileTable.write(1);
					recordOffset = (short) (recordOffset - 8);
					fileTable.writeShort(recordOffset);
					fileTable.writeInt(-1);
					fileTable.writeShort(recordOffset);
					fileTable.seek(recordOffset);
					fileTable.writeInt(child + 1);
					fileTable.writeInt(key);
				} else {
					fileTable.seek((parent - 1) * DatabaseConstants.PAGE_SIZE + 2);
					recordOffset = fileTable.readShort();
					recordOffset = (short) (recordOffset - 8);
					fileTable.seek(recordOffset);
					fileTable.writeInt(child + 1);
					fileTable.writeInt(key);
					fileTable.seek((parent - 1) * DatabaseConstants.PAGE_SIZE + 2);
					fileTable.writeShort(recordOffset);
					for (int recordCount = 0; recordCount < noOfRecords; recordCount++) {
						fileTable.seek((parent - 1) * DatabaseConstants.PAGE_SIZE + 8 + 2 * recordCount);
						offset = fileTable.readShort();
						fileTable.seek(offset + 4);
						int rowId = fileTable.readInt();
						recordMap.put(rowId, offset);
					}
					recordMap.put(key, recordOffset);
					fileTable.seek((parent - 1) * DatabaseConstants.PAGE_SIZE + 1);
					fileTable.write(noOfRecords++);
					fileTable.seek((parent - 1) * DatabaseConstants.PAGE_SIZE + 8);
					for (Entry<Integer, Short> entry : recordMap.entrySet()) {
						fileTable.writeShort(entry.getValue());
					}
				}
			} else {
				/* Split the interior page */
				int newInteriorPage = createInteriorPage(fileTable);
				int interiorKey = splitKeyLocation(fileTable, parent - 1);
				int mid = (int) Math.ceil(noOfRecords / 2);
				int recordOffset = DatabaseConstants.PAGE_SIZE;
				for (int i = mid - 1; i < noOfRecords; i++) {
					fileTable.seek((parent - 1) * DatabaseConstants.PAGE_SIZE + 8);
					offset = fileTable.readShort();
					fileTable.seek(offset);
					byte[] payLoadContent = new byte[8];
					fileTable.read(payLoadContent);
					recordOffset = recordOffset - 8;
					fileTable.seek((newInteriorPage - 1) * DatabaseConstants.PAGE_SIZE + recordOffset);
					fileTable.write(payLoadContent);

					/* Write the offset in page header */
					fileTable.seek((newInteriorPage - 1) * DatabaseConstants.PAGE_SIZE + (i - (mid - 1)) * 2);
					fileTable.writeShort(recordOffset);
				}
				fileTable.seek((parent - 1) * DatabaseConstants.PAGE_SIZE + 1);
				fileTable.write(mid - 1);

				fileTable.seek((newInteriorPage - 1) * DatabaseConstants.PAGE_SIZE + 1);
				fileTable.write(noOfRecords - (mid - 1));
				/* Update Parent */
				int interiorParent = getParent(fileTable, parent);
				if (interiorParent == -1) {
					interiorParent = createInteriorPage(fileTable);
					setParent(fileTable, interiorParent, parent, interiorKey);
					fileTable.seek((interiorParent - 1) * DatabaseConstants.PAGE_SIZE + 4);
					fileTable.writeInt(newInteriorPage);
				} else {
					fileTable.seek((interiorParent - 1) * DatabaseConstants.PAGE_SIZE + 4);
					if (fileTable.readInt() == parent) {
						setParent(fileTable, interiorParent, parent, interiorKey);
						fileTable.seek((parent - 1) * DatabaseConstants.PAGE_SIZE + 4);
						fileTable.writeInt(newInteriorPage);
					} else {
						setParent(fileTable, interiorParent, parent, interiorKey);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/*
	 * Function to get interior page
	 */
	
	private static int createInteriorPage(RandomAccessFile fileTable) {
		int interiorPageNo = 0;
		try {
			interiorPageNo = (int)(fileTable.length()/DatabaseConstants.PAGE_SIZE);
			interiorPageNo++;
			fileTable.setLength(fileTable.length() + DatabaseConstants.PAGE_SIZE);
			fileTable.seek((interiorPageNo - 1) * DatabaseConstants.PAGE_SIZE);
			fileTable.writeByte(0x05);
		} catch(Exception e) {
			e.printStackTrace();
		}
		return interiorPageNo;
	}


	/*
	 * Function to get parent page for the new page
	 */
	
	private static int getParent(RandomAccessFile fileTable, int page) {
		try {
			int noOfPages = (int)(fileTable.length()/DatabaseConstants.PAGE_SIZE);
			for(int pageCount = 0; pageCount < noOfPages; pageCount++) {
				fileTable.seek(pageCount * DatabaseConstants.PAGE_SIZE);
				byte pageType = fileTable.readByte();
				if(pageType == 0x05) {
					fileTable.seek(pageCount * DatabaseConstants.PAGE_SIZE + 4);
					if(fileTable.readInt() == page) {
						return pageCount + 1;
					}
					fileTable.seek(pageCount * DatabaseConstants.PAGE_SIZE + 1);
					int noOfRecords = fileTable.read();
					for(int recordCount = 0; recordCount < noOfRecords; recordCount++) {
							fileTable.seek(pageCount * DatabaseConstants.PAGE_SIZE + 8 + 2 * recordCount);
							int recordOffset = fileTable.readShort();
							fileTable.seek(recordOffset);
							if(fileTable.readInt() == page) {
								return recordCount + 1;
							}
						}
					}
					
				}
			
		} catch(Exception e) {
			e.printStackTrace();
		}
		return 0;
	}


	/*
	 * Function to split data
	 */
	private static int splitKeyLocation(RandomAccessFile fileTable, int pageNo) {
		int key = 0;
		try {
			fileTable.seek(pageNo * DatabaseConstants.PAGE_SIZE);
			byte pageType = fileTable.readByte();
			byte noOfRecords = fileTable.readByte();
			short midRecord = (short) Math.ceil(noOfRecords / 2);
			fileTable.seek(pageNo * DatabaseConstants.PAGE_SIZE + 8 + (2 * (midRecord - 1)));
			short offset = fileTable.readShort();
			fileTable.seek(offset);
			
			/* Page is a leaf page */
			if (pageType == 0x0D) {
				fileTable.seek(offset + 2);
			} else {
				fileTable.seek(offset + 4);
			}
			key = fileTable.readInt();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return key;
	}

	/*
	 * Function to write payload to file
	 */

	@SuppressWarnings("deprecation")
	private static void writePayLoadToFile(RandomAccessFile fileTable, Record record, int recordLocation) {
		
		try {
			fileTable.seek(recordLocation);
			fileTable.writeShort(record.getPayLoadLength());
			fileTable.writeInt(record.getRowId());
			fileTable.writeByte(record.getNoOfColumns());
			byte[] dataType = record.getDataTypeOfColumn();
			fileTable.write(dataType);
			String s;
			String[] payLoadContent = record.getPayLoadContent();
			for(int i = 0; i < dataType.length; i++) {
				switch(dataType[i]) {
					case 0x00: 
						fileTable.writeByte(0);
						break;
					case 0x01: 
						fileTable.writeShort(0);
						break;
					case 0x02: 
						fileTable.writeInt(0);
						break;
					case 0x03: 
						fileTable.writeLong(0);
						break;
					case 0x04: 
						fileTable.writeByte(new Byte(payLoadContent[i + 1]));
						break;
					case 0x05: 
						fileTable.writeShort(new Short(payLoadContent[i + 1]));
						break;
					case 0x06: 
						fileTable.writeInt(new Integer(payLoadContent[i + 1]));
						break;
					case 0x07: 
						fileTable.writeLong(new Long(payLoadContent[i + 1]));
						break;
					case 0x08: 
						fileTable.writeFloat(new Float(payLoadContent[i + 1]));
						break;
					case 0x09: 
						fileTable.writeDouble(new Double(payLoadContent[i + 1]));
						break;
					case 0x0A:
						s = payLoadContent[i + 1];	
						Date temp = new SimpleDateFormat(DatabaseConstants.DATEPATTERN).parse(s);
						long time = temp.getTime();
						fileTable.writeLong(time);
						break;
					case 0x0B:
					    s = payLoadContent[i + 1];	
						s = s+"_00:00:00";
						Date temp1 = new SimpleDateFormat(DatabaseConstants.DATEPATTERN).parse(s);
						long time1 = temp1.getTime();
						fileTable.writeLong(time1);
						break;
					default:
						fileTable.writeBytes(payLoadContent[i + 1]);
						break;
				}
			}
			
			/* Update Page Header */
			fileTable.seek((DatabaseConstants.PAGE_SIZE * record.getPageNo()) + 1);
			int noOfRecords = fileTable.readByte();
			fileTable.seek((DatabaseConstants.PAGE_SIZE * record.getPageNo()) + 1);
			fileTable.write((byte)(noOfRecords + 1));
			short offset = -1;
			int rowId = 0;
			TreeMap<Integer, Short> offsetMap = new TreeMap<Integer, Short>();
			for(int offsetCount = 0; offsetCount < noOfRecords; offsetCount++) {
				fileTable.seek((DatabaseConstants.PAGE_SIZE * record.getPageNo()) + 8 + (offsetCount * 2));
				offset = fileTable.readShort();
				fileTable.seek(offset + 2);
				rowId = fileTable.readInt();
				offsetMap.put(rowId, offset);
			}
			offsetMap.put(record.getRowId(), (short)recordLocation);
			
			/* Sort in Ascending Order */
			fileTable.seek((DatabaseConstants.PAGE_SIZE * record.getPageNo()) + 8);
			for (Map.Entry<Integer, Short> entry : offsetMap.entrySet()) {
				offset = entry.getValue();
				fileTable.writeShort(offset);
			}
			fileTable.seek((DatabaseConstants.PAGE_SIZE * record.getPageNo()) + 2);
			fileTable.writeShort(recordLocation);
			fileTable.close();
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * Function to insert New Record
	 */

	private static Record insertRecord(int pageNo, int rowId, short payLoadLength, byte[] payLoadDataType,
			String[] values) {

		Record record = new Record();
		record.setPageNo(pageNo);
		record.setRowId(rowId);
		record.setPayLoadLength(payLoadLength);
		record.setNoOfColumns(Byte.parseByte(values.length - 1+ ""));
		record.setDataTypeOfColumn(payLoadDataType);
		record.setPayLoadContent(values);
		return record;
	}

	/*
	 * Function to get offset of the new Record in the file
	 */

	private static int getRecordOffset(RandomAccessFile file, int pageNo, int payLoadSize) {
		int recordOffset = 0;
		short dataStartAddress;
		try {
			file.seek((pageNo * DatabaseConstants.PAGE_SIZE) + 2);
			dataStartAddress = file.readShort();
			if (dataStartAddress == 0) {
				recordOffset = DatabaseConstants.PAGE_SIZE - payLoadSize;
			} else {
				file.seek((pageNo * DatabaseConstants.PAGE_SIZE) + 1);
				int noOfRecords = file.readByte();
				int pageHeaderLength = 8 + 2 * noOfRecords + 2;
				file.seek((pageNo * DatabaseConstants.PAGE_SIZE) + 2);
				dataStartAddress = (short) ((pageNo + 1) * DatabaseConstants.PAGE_SIZE - file.readShort());
				int usedSpace = dataStartAddress + pageHeaderLength;
				int availableSpace = DatabaseConstants.PAGE_SIZE - usedSpace;
				if (availableSpace >= payLoadSize) {
					file.seek((pageNo * DatabaseConstants.PAGE_SIZE) + 2);
					dataStartAddress = file.readShort();
					recordOffset = dataStartAddress - payLoadSize;
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return recordOffset;
	}

	/*
	 * Function to get datavalues of one page
	 */
	private static Set<Record> getDataValues(String tableName, String[] columnNames, String[] condition,
			String filePath) {
		try {
			RandomAccessFile filePage = new RandomAccessFile(filePath, "rw");
			int noOfPages = (int) (filePage.length() / DatabaseConstants.PAGE_SIZE);

			Set<Page> pageSet = new LinkedHashSet<Page>();
			Set<Record> recordSet = new LinkedHashSet<Record>();
			Map<Integer, String> colNamesMap = getColumnsFromDavisbaseColumns(tableName);
			for (int pageCount = 0; pageCount < noOfPages; pageCount++) {
				filePage.seek(DatabaseConstants.PAGE_SIZE * pageCount);
				byte pageType = filePage.readByte() ;
				if (pageType == 0x0D) {
					Page page = new Page();
					page.setPageNo(pageCount);
					page.setPageType(pageType);
					int noOfRecords = filePage.readByte();
					short[] recordOffset = new short[noOfRecords];
					filePage.seek((DatabaseConstants.PAGE_SIZE * pageCount) + 8);
					for (int recLocation = 0; recLocation < noOfRecords; recLocation++) {
						recordOffset[recLocation] = filePage.readShort();
					}
					recordSet = getRecords(filePage, recordOffset, pageCount);
					page.setPageRecords(recordSet);
					pageSet.add(page);
				}
			}
			filePage.close();
			if (condition.length > 0) {
				Set<Record> filteredRecords = filterRecords(colNamesMap, recordSet, condition);
				return filteredRecords;
			} else {
				return recordSet;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/*
	 * Function to get column Names from davisbase columns
	 */

	public static Map<Integer, String> getColumnsFromDavisbaseColumns(String tableName) {
		Map<Integer, String> colNamesMap = new HashMap<Integer, String>();
		try {
			RandomAccessFile fileTable = new RandomAccessFile("data/meta_data/davisbase_columns.tbl", "rw");
			int noOfPages = (int) (fileTable.length() / DatabaseConstants.PAGE_SIZE);
			for (int pageCount = 0; pageCount < noOfPages; pageCount++) {
				fileTable.seek(DatabaseConstants.PAGE_SIZE * pageCount);
				if (fileTable.readByte() == 0x0D) {
					int noOfRecords = fileTable.readByte();
					short[] recordOffset = new short[noOfRecords];
					fileTable.seek((DatabaseConstants.PAGE_SIZE * pageCount) + 8);
					for (int recLocation = 0; recLocation < noOfRecords; recLocation++) {
						recordOffset[recLocation] = fileTable.readShort();
					}
					Set<Record> recordSet = new LinkedHashSet<Record>();
					recordSet = getRecords(fileTable, recordOffset, pageCount);
					for (Record rec : recordSet) {
						String[] payLoadContent = rec.getPayLoadContent();
						if (payLoadContent[0].equalsIgnoreCase(tableName)) {
							colNamesMap.put(Integer.parseInt(payLoadContent[3]), payLoadContent[1]);
						}
					}
				}
			}
			fileTable.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return colNamesMap;
	}

	/*
	 * Function to get pageNo
	 */
	private static int getPageNo(String tableName, int key, String filePath) {
		try {
			RandomAccessFile fileTable = new RandomAccessFile(filePath, "rw");
			int noOfPages = (int) (fileTable.length() / DatabaseConstants.PAGE_SIZE);
			for (int pageCount = 0; pageCount < noOfPages; pageCount++) {
				fileTable.seek(DatabaseConstants.PAGE_SIZE * pageCount);
				if (fileTable.readByte() == 0x0D) {
					int noOfRecords = fileTable.readByte();
					short[] recordOffset = new short[noOfRecords];
					fileTable.seek((DatabaseConstants.PAGE_SIZE * pageCount) + 8);
					for (int recLocation = 0; recLocation < noOfRecords; recLocation++) {
						recordOffset[recLocation] = fileTable.readShort();
					}
					Set<Record> recordSet = new LinkedHashSet<Record>();
					recordSet = getRecords(fileTable, recordOffset, pageCount);
					if (recordSet.size() != 0) {
						List<Record> recordList = new ArrayList<Record>(recordSet);
						Collections.sort(recordList, new RowIdComparator());
						Record lastRecord = recordList.get(recordList.size() - 1);
						fileTable.seek((DatabaseConstants.PAGE_SIZE * pageCount) + 4);
						int filePointer = fileTable.readInt();
						if (recordList.get(0).getRowId() <= key && key <= lastRecord.getRowId()) {
							return pageCount;
						} else if (filePointer == -1 && lastRecord.getRowId() < key) {
							return pageCount;
						}

					} else {
						return 0;
					}
				}
			}
			fileTable.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	/*
	 * Function to get columns of file davisbaseColumns
	 */
	private static Set<Record> getColumnsMeta(String[] columnNames, String[] condition) {

		try {
			RandomAccessFile davisbaseCols = new RandomAccessFile("data/meta_data/davisbase_columns.tbl", "rw");
			int noOfPages = (int) (davisbaseCols.length() / DatabaseConstants.PAGE_SIZE);

			Map<Integer, String> colNamesMap = new HashMap<Integer, String>();
			Set<Record> recordSet = new LinkedHashSet<Record>();
			for (int i = 0; i < noOfPages; i++) {
				davisbaseCols.seek(DatabaseConstants.PAGE_SIZE * i);
				byte pageType = davisbaseCols.readByte();
				if (pageType == 0x0D) {
					int noOfRecords = davisbaseCols.readByte();
					short[] recordOffset = new short[noOfRecords];
					davisbaseCols.seek((DatabaseConstants.PAGE_SIZE * i) + 8);
					for (int offset = 0; offset < noOfRecords; offset++) {
						recordOffset[offset] = davisbaseCols.readShort();
					}
					
					recordSet = getRecords(davisbaseCols, recordOffset, i);
					/* Extract col name from records */
					for (Record rec : recordSet) {
						String[] payLoadContent = rec.getPayLoadContent();
						if (payLoadContent[0].equalsIgnoreCase("davisbase_columns")) {
							colNamesMap.put(Integer.parseInt(payLoadContent[3]), payLoadContent[1]);
						}
					}
				}
			}
			davisbaseCols.close();
			if (condition.length > 0) {
				Set<Record> filteredRecords = filterRecords(colNamesMap, recordSet, condition);
				return filteredRecords;
			} else {
				return recordSet;
			}
		}

		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/*
	 * Function to get dataType of columns
	 */

	private static Set<Record> filterRecords(Map<Integer, String> colNamesMap, Set<Record> recordSet,
			String[] condition) {
		Set<Record> filteredRecordsSet = new LinkedHashSet<Record>();
		int columnPosition = 2;
		for (Map.Entry<Integer, String> entry : colNamesMap.entrySet()) {
			if (entry.getValue().equals(condition[0])) {
				columnPosition = entry.getKey(); // Table_Name
				break;
			}
		}

		for (Record records : recordSet) {
			byte[] dataTypeCodes = records.getDataTypeOfColumn();
			boolean result;

			if (columnPosition == 1) {
				result = checkDataType((byte) 0x06, String.valueOf(records.getRowId()), condition);
			} else
				result = checkDataType(dataTypeCodes[columnPosition - 2],
						records.getPayLoadContent()[columnPosition - 2], condition);

			if (result) {
				filteredRecordsSet.add(records);
			}
		}

		return filteredRecordsSet;
	}

	/*
	 * Function to get dataType of columns
	 */
	public static String[] getDataType(Set<Record> columnsMeta) {
		String[] dataType = new String[columnsMeta.size()];
		int dataTypeCount = 0;
		for (Record rec : columnsMeta) {
			dataType[dataTypeCount] = rec.getPayLoadContent()[2];
			dataTypeCount++;
		}
		return dataType;
	}

	/*
	 * Function to check if value is NULL
	 */

	private static String[] getIsNull(Set<Record> columnsMeta) {
		String[] isNull = new String[columnsMeta.size()];
		int isNullCount = 0;
		for (Record rec : columnsMeta) {
			isNull[isNullCount] = rec.getPayLoadContent()[4];
			isNullCount++;
		}
		return isNull;
	}

	/*
	 * Function to get records and return MAP
	 */
	public static Set<Record> getRecords(RandomAccessFile fileTable, short[] recordOffset, int pageNo) {

		Set<Record> recordSet = new LinkedHashSet<Record>();

		for (int position = 0; position < recordOffset.length; position++) {
			try {
				SimpleDateFormat formater = new SimpleDateFormat(DatabaseConstants.DATEPATTERN);
				long dateLength;
				Record record = new Record();
				record.setPageNo(pageNo);
				record.setRecordLocation(recordOffset[position]);
				fileTable.seek(recordOffset[position]);
				record.setPayLoadLength(fileTable.readShort());
				int rowId = fileTable.readInt();
				record.setRowId(rowId);

				byte noOfColumns = fileTable.readByte();
				record.setNoOfColumns(noOfColumns);
				
				byte[] dataType = new byte[noOfColumns];
				int bytesRead = fileTable.read(dataType);
				record.setDataTypeOfColumn(dataType);

				String[] payLoad = new String[noOfColumns];
				record.setPayLoadContent(payLoad);

				for (int i = 0; i < noOfColumns; i++) {
					switch (dataType[i]) {

					case 0x00:
						payLoad[i] = Integer.toString(fileTable.readByte());
						payLoad[i] = "null";
						break;

					case 0x01:
						payLoad[i] = Integer.toString(fileTable.readShort());
						payLoad[i] = "null";
						break;

					case 0x02:
						payLoad[i] = Integer.toString(fileTable.readInt());
						payLoad[i] = "null";
						break;

					case 0x03:
						payLoad[i] = Long.toString(fileTable.readLong());
						payLoad[i] = "null";
						break;

					case 0x04:
						payLoad[i] = Integer.toString(fileTable.readByte());
						break;

					case 0x05:
						payLoad[i] = Integer.toString(fileTable.readShort());
						break;

					case 0x06:
						payLoad[i] = Integer.toString(fileTable.readInt());
						break;

					case 0x07:
						payLoad[i] = Long.toString(fileTable.readLong());
						break;

					case 0x08:
						payLoad[i] = String.valueOf(fileTable.readFloat());
						break;

					case 0x09:
						payLoad[i] = String.valueOf(fileTable.readDouble());
						break;

					case 0x0A:
						dateLength = fileTable.readLong();
						Date dateTime = new Date(dateLength);
						payLoad[i] = formater.format(dateTime);
						break;

					case 0x0B:
						dateLength = fileTable.readLong();
						Date date = new Date(dateLength);
						payLoad[i] = formater.format(date).substring(0, 10);
						break;

					default:
						int textlength = new Integer(dataType[i] - 0x0C);
						byte[] bytes = new byte[textlength];
						for (int j = 0; j < textlength; j++)
							bytes[j] = fileTable.readByte();
						payLoad[i] = new String(bytes);
						break;
					}
				}
				recordSet.add(record);
			} catch (Exception e) {
				System.out.println("Error at creating table");
				e.printStackTrace();
			}
		}
		return recordSet;

	}

	/*
	 * Function to update table
	 */
	
	public static void updateTable(String tableName, String[] setValue, String[] whereClause) {
		try {
			String filePath = "";
			if (tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns")) {
				filePath = "data/meta_data/" + tableName + ".tbl";
			} else {
				filePath = "data/" + DatabaseConstants.DEFAULTDB + "/" + tableName + ".tbl";
			}
			RandomAccessFile fileTable = new RandomAccessFile(filePath, "rw");
			int noOfPages = (int)(fileTable.length() / DatabaseConstants.PAGE_SIZE);
			String condition[] = { "table_name", "=", tableName };
			String columnNames[] = { "*" };
			Map<Integer, String> columnNamesMap = getColumnsFromDavisbaseColumns(tableName);
			int key = -1; //ordinal position
			int noOfRecords = 0;
			short[] recordOffset = new short[0];
			int rowId = 0;
			int noOfColumn;
			byte[] serialCode = new byte[0];
			byte serialCodeToUpdate;
			int offsetCount = 0;
			int position = 0;
			for (Map.Entry<Integer, String> entry : columnNamesMap.entrySet()) {
				String columnName = entry.getValue();
				if (columnName.equals(setValue[0])) {
					key = entry.getKey();
				}
			}			
			if(whereClause.length > 0) {
				int conditionValue = Integer.parseInt(whereClause[2]);
				condition = new String[0];
				boolean flag = false;
				int pageNo = getPageNo(tableName, conditionValue, filePath);
				/* Check for duplicate values for primary key */
				Set<Record> dataSet = getDataValues(tableName, columnNames, condition, filePath);
				for (Record r : dataSet) {
					if (r.getRowId() == conditionValue) {
						flag = true;
						break;
					}
				}
				if(flag == true) {
					try {
						fileTable.seek(pageNo * DatabaseConstants.PAGE_SIZE + 1);
						noOfRecords = fileTable.read();;
						recordOffset = new short[noOfRecords];
						for(offsetCount = 0; offsetCount < noOfRecords; offsetCount++) {
							fileTable.seek(pageNo * DatabaseConstants.PAGE_SIZE + 8 + (2 * offsetCount));
							recordOffset[offsetCount] = fileTable.readShort();	
							fileTable.seek(recordOffset[offsetCount] + 2);
							rowId = fileTable.readInt();
							
							if(rowId == conditionValue) {
								noOfColumn = fileTable.read();
								serialCode = new byte[noOfColumn];
								fileTable.read(serialCode);
								position = 0;
								for(int i = 0; i < key - 2; i++) {
									position = position + getDataLength(serialCode[i]);
								}
						
								fileTable.seek(recordOffset[offsetCount] + DatabaseConstants.PAYLOAD_HEADER_SIZE + noOfColumn + 1 + position);
								serialCodeToUpdate = serialCode[key - 2];
								switch(serialCodeToUpdate) {

								case 0x00:	
									fileTable.write(Integer.parseInt(setValue[2]));
									serialCode[key - 2] = 0x04;
									break;
								case 0x01:	
									fileTable.writeShort(Integer.parseInt(setValue[2]));
									serialCode[key -2] = 0x05;
									break;
								case 0x02:	
									fileTable.writeInt(Integer.parseInt(setValue[2]));
									serialCode[key - 2]=0x06;
									break;
								case 0x03:	
									fileTable.writeDouble(Double.parseDouble(setValue[2]));
									serialCode[key - 2] = 0x09;
									break;
								case 0x04:	
									fileTable.write(Integer.parseInt(setValue[2]));
									break;
								case 0x05:	
									fileTable.writeShort(Integer.parseInt(setValue[2]));
									break;
								case 0x06:	
									fileTable.writeInt(Integer.parseInt(setValue[2]));
									break;
								case 0x07:
									fileTable.writeLong(Long.parseLong(setValue[2]));
									break;
								case 0x08: 	
									fileTable.writeFloat(Float.parseFloat(setValue[2]));
									break;
								case 0x09:
									fileTable.writeDouble(Double.parseDouble(setValue[2]));
									break;

								}
								fileTable.seek(recordOffset[offsetCount] + 7);
								fileTable.write(serialCode);
							}

						}
					} catch(Exception e) {
						e.printStackTrace(System.out);				
					}
				}
			}
			else {
				try {
					noOfPages = (int)(fileTable.length() / DatabaseConstants.PAGE_SIZE);
					for(int pageCount = 0; pageCount < noOfPages; pageCount++) {
						fileTable.seek(pageCount * DatabaseConstants.PAGE_SIZE + 1);
						noOfRecords = fileTable.read();
						recordOffset = new short[noOfRecords];
						for(offsetCount = 0; offsetCount < noOfRecords; offsetCount++) {
							fileTable.seek(pageCount * DatabaseConstants.PAGE_SIZE + 8 + (2 * offsetCount));
							recordOffset[offsetCount] = fileTable.readShort();	
							fileTable.seek(recordOffset[offsetCount] + DatabaseConstants.PAYLOAD_HEADER_SIZE);
							noOfColumn = fileTable.read();
							serialCode = new byte[noOfColumn];
							fileTable.read(serialCode);
							position = 0;
							for(int i = 0; i < key - 2; i++) {
								position = position + getDataLength(serialCode[i]);
							}
							fileTable.seek(recordOffset[offsetCount] + DatabaseConstants.PAYLOAD_HEADER_SIZE + noOfColumn + 1 + position);
							serialCodeToUpdate = serialCode[key - 2];
							
							switch(serialCodeToUpdate) {

							case 0x00:	
								fileTable.write(Integer.parseInt(setValue[2]));
								serialCode[key - 2] = 0x04;
								break;
							case 0x01:	
								fileTable.writeShort(Integer.parseInt(setValue[2]));
								serialCode[key -2] = 0x05;
								break;
							case 0x02:	
								fileTable.writeInt(Integer.parseInt(setValue[2]));
								serialCode[key - 2] = 0x06;
								break;
							case 0x03:	
								fileTable.writeDouble(Double.parseDouble(setValue[2]));
								serialCode[key - 2] = 0x09;
								break;
							case 0x04:	
								fileTable.write(Integer.parseInt(setValue[2]));
								break;
							case 0x05:	
								fileTable.writeShort(Integer.parseInt(setValue[2]));
								break;
							case 0x06:	
								fileTable.writeInt(Integer.parseInt(setValue[2]));
								break;
							case 0x07:
								fileTable.writeLong(Long.parseLong(setValue[2]));
								break;
							case 0x08: 	
								fileTable.writeFloat(Float.parseFloat(setValue[2]));
								break;
							case 0x09:
								fileTable.writeDouble(Double.parseDouble(setValue[2]));
								break;

							}
							fileTable.seek(recordOffset[offsetCount] + 7);
							fileTable.write(serialCode);
						}
					}
				} catch(Exception e) {
					e.printStackTrace();				
				}
			}
			
			fileTable.close();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	

	/*
	 * Function to convert datatype to serial type code
	 */

	private static byte getSerialCode(String value, String dataType) {
		if (value.equals("null")) {
			switch (dataType) {
			case "TINYINT":
				return 0x00;
			case "SMALLINT":
				return 0x01;
			case "INT":
				return 0x02;
			case "BIGINT":
				return 0x03;
			case "REAL":
				return 0x02;
			case "DOUBLE":
				return 0x03;
			case "DATETIME":
				return 0x03;
			case "DATE":
				return 0x03;
			case "TEXT":
				return 0x03;
			default:
				return 0x00;
			}
		} else {
			switch (dataType) {
			case "TINYINT":
				return 0x04;
			case "SMALLINT":
				return 0x05;
			case "INT":
				return 0x06;
			case "BIGINT":
				return 0x07;
			case "REAL":
				return 0x08;
			case "DOUBLE":
				return 0x09;
			case "DATETIME":
				return 0x0A;
			case "DATE":
				return 0x0B;
			case "TEXT":
				return (byte) (value.length() + 0x0C);
			default:
				return 0x00;
			}
		}
	}

	/*
	 * Function to return length of byte codes
	 */

	private static short getDataLength(byte codes) {
		switch (codes) {
		case 0x00:
			return 1;
		case 0x01:
			return 2;
		case 0x02:
			return 4;
		case 0x03:
			return 8;
		case 0x04:
			return 1;
		case 0x05:
			return 2;
		case 0x06:
			return 4;
		case 0x07:
			return 8;
		case 0x08:
			return 4;
		case 0x09:
			return 8;
		case 0x0A:
			return 8;
		case 0x0B:
			return 8;
		default:
			return (short) (codes - 0x0C);
		}
	}

	private static boolean checkDataType(byte code, String data, String[] condition) {

		if (code >= 0x04 && code <= 0x07) {
			Long dataLong = Long.parseLong(data);
			switch (condition[1]) {
			case "=":
				if (dataLong == Long.parseLong(condition[2]))
					return true;
				break;
			case ">":
				if (dataLong > Long.parseLong(condition[2]))
					return true;
				break;
			case "<":
				if (dataLong < Long.parseLong(condition[2]))
					return true;
				break;
			case "<=":
				if (dataLong <= Long.parseLong(condition[2]))
					return true;
				break;
			case ">=":
				if (dataLong >= Long.parseLong(condition[2]))
					return true;
				break;
			case "<>":
				if (dataLong != Long.parseLong(condition[2]))
					return true;
				break;
			default:
				System.out.println("undefined operator return false");
				return false;
			}

		} else if (code == 0x08 || code == 0x09) {
			Double doubleData = Double.parseDouble(data);
			switch (condition[1]) {
			case "=":
				if (doubleData == Double.parseDouble(condition[2]))
					return true;
				break;
			case ">":
				if (doubleData > Double.parseDouble(condition[2]))
					return true;
				break;
			case "<":
				if (doubleData < Double.parseDouble(condition[2]))
					return true;
				break;
			case "<=":
				if (doubleData <= Double.parseDouble(condition[2]))
					return true;
				break;
			case ">=":
				if (doubleData >= Double.parseDouble(condition[2]))
					return true;
				break;
			case "<>":
				if (doubleData != Double.parseDouble(condition[2]))
					return true;
				break;
			default:
				System.out.println("undefined operator return false");
				return false;
			}

		} else if (code >= 0x0C) {

			condition[2] = condition[2].replaceAll("'", "");
			condition[2] = condition[2].replaceAll("\"", "");
			switch (condition[1]) {
			case "=":
				if (data.equalsIgnoreCase(condition[2]))
					return true;
				break;
			case "<>":
				if (!data.equalsIgnoreCase(condition[2]))
					return true;
				break;
			default:
				System.out.println("undefined operator return false");
				return false;
			}
		}

		return false;

	}

	/*
	 * Function to select values from the table
	 */
	
	public static void selectFromTable(String tableName, String[] columnNames, String[] condition) {
		try {
			String filePath = "";
			if (tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns")) {
				filePath = "data/meta_data/" + tableName + ".tbl";
			} else {
				filePath = "data/" + DatabaseConstants.DEFAULTDB + "/" + tableName + ".tbl";
			}
			RandomAccessFile fileTable = new RandomAccessFile(filePath, "rw");
			int noOfPages = (int)(fileTable.length() / DatabaseConstants.PAGE_SIZE);
			
			Map<Integer, String> columnNamesMap = getColumnsFromDavisbaseColumns(tableName);
			Set<Record> recordSet = new LinkedHashSet<Record>();
			for(int pageCount = 0; pageCount < noOfPages; pageCount++) {
				fileTable.seek(pageCount * DatabaseConstants.PAGE_SIZE);
				if(fileTable.readByte() == 0x0D) {
					int noOfRecords = fileTable.readByte();
					short[] recordOffset = new short[noOfRecords];
					fileTable.seek(pageCount * DatabaseConstants.PAGE_SIZE + 8);
					for(int offsetCount = 0; offsetCount < noOfRecords; offsetCount++) {
						recordOffset[offsetCount] = fileTable.readShort();					
					}
					recordSet = getRecords(fileTable, recordOffset, pageCount);
				}
			}
			if (condition.length > 0) {
				Set<Record> filteredRecords = filterRecords(columnNamesMap, recordSet, condition);
				displayTable(columnNamesMap, columnNames, filteredRecords);	
			} else {
				displayTable(columnNamesMap, columnNames, recordSet);
			}
		}
		catch(Exception e) {
			System.out.println("Error in select query");
			e.printStackTrace();
		}
	}

	/* 
	 * Function to display records 
	 */
	
	private static void displayTable(Map<Integer, String> columnNamesMap, String[] columnNames, Set<Record> recordSet) {
		String colHeader = " ";
		String values = " ";
		/* To display all columns */
		if (columnNames[0].equals("*")) {
			for (Map.Entry<Integer, String> entry : columnNamesMap.entrySet()) {
				colHeader += entry.getValue() + " | ";
			}
			System.out.println(colHeader);

			for (Record rec : recordSet) {
				values += rec.getRowId();
				String[] payLoad = rec.getPayLoadContent();
				for (String payLoadValue : payLoad) {
					values = values + "   |   " + payLoadValue;
				}
				System.out.println(values);
				values = " ";
			}
		}
		/* To display only selective columns */
		else {
			for (Map.Entry<Integer, String> entry : columnNamesMap.entrySet()) {
				for (String insertCols : columnNames) {
					if (insertCols.equalsIgnoreCase(entry.getValue())) {
						colHeader += entry.getValue() + " | ";
					}
				}
			}
			System.out.println(colHeader);
			for (Record rec : recordSet) {
				String[] payLoad = rec.getPayLoadContent();
				for (Map.Entry<Integer, String> entry : columnNamesMap.entrySet()) {
					for (String insertCols : columnNames) {
						if (insertCols.equalsIgnoreCase(entry.getValue())) {
							if (entry.getKey() == 1) {
								values = values + rec.getRowId() + "   |   ";
							} else {
								values = values + payLoad[entry.getKey() - 2] + "   |   ";
							}
						}
					}
				}
				System.out.println(values);
				values = " ";
			}
		}

	}

	/* 
	 * Function to drop Table 
	 */
	
	public static void dropTable(String tableName) {
		try {
			RandomAccessFile davisbaseTables = new RandomAccessFile("data/meta_data/davisbase_tables.tbl", "rw");
			dropTableFromMetaData(davisbaseTables, "davisbase_tables", tableName);
			RandomAccessFile davisbaseColumns = new RandomAccessFile("data/meta_data/davisbase_columns.tbl", "rw");
			dropTableFromMetaData(davisbaseColumns, "davisbase_columns", tableName);	
		} catch(Exception e) {
			e.printStackTrace();
		}
		
	}

	/* 
	 * Function to delete all records of the table to be dropped 
	 */
	
	private static void dropTableFromMetaData(RandomAccessFile fileTable, String string, String tableName) {
		try {
			int noOfPages = (int)(fileTable.length() / DatabaseConstants.PAGE_SIZE);
			Map<Integer, String> columnNamesMap = getColumnsFromDavisbaseColumns(tableName);
			Set<Record> recordSet = new LinkedHashSet<Record>();
			for(int pageCount = 0; pageCount < noOfPages; pageCount++) {
				fileTable.seek(pageCount * DatabaseConstants.PAGE_SIZE);
				if(fileTable.readByte() == 0x0D) {
					int noOfRecords = fileTable.readByte();
					short[] recordOffset = new short[noOfRecords];
					fileTable.seek(pageCount * DatabaseConstants.PAGE_SIZE + 8);
					for(int offsetCount = 0; offsetCount < noOfRecords; offsetCount++) {
						recordOffset[offsetCount] = fileTable.readShort();					
					}
					recordSet = getRecords(fileTable, recordOffset, pageCount);
					String[] condition = {"table_name", "<>", tableName};
					
					Set<Record> filteredRecords = filterRecords(columnNamesMap, recordSet, condition);
					short[] offset = new short[filteredRecords.size()];
					int offsetCount = 0;
					for(Record rec : filteredRecords) {
						offset[offsetCount] = rec.getRecordLocation();
						fileTable.seek((pageCount * DatabaseConstants.PAGE_SIZE) + 8 + (2 * offsetCount));
						fileTable.writeShort(offset[offsetCount]);
						offsetCount++;
					}

					fileTable.seek((pageCount * DatabaseConstants.PAGE_SIZE)+1);
					fileTable.writeByte(offset.length);
					fileTable.writeShort(offset[offset.length-1]);
				}
			}
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}

class RowIdComparator implements Comparator<Record> {
	@Override
	public int compare(Record a, Record b) {
		return a.rowId < b.rowId ? -1 : a.rowId == b.rowId ? 0 : 1;
	}
}
