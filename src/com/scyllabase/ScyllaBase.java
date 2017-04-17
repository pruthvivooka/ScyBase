package com.scyllabase;

import javax.rmi.CORBA.Util;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ScyllaBase {

	private static boolean isExit = false;
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

	public static void main(String[] args) {
		splashScreen();
		initializeDatabaseInfo();
		//parseCreateString("create table parts (id INT PRIMARY KEY, description TEXT, availability INT NOT NULL)");
		/*
		String userCommand = "";
		while(!isExit) {
			System.out.print("scysql> ");
			userCommand = scanner.next().replace("\n", " ").replace("\r", " ").trim().toLowerCase();
			Pattern multi_spaces_pattern = Pattern.compile("\\s+(?=(?:[^\\'\"]*[\\'\"][^\\'\"]*[\\'\"])*[^\\'\"]*$)");
 			Matcher m = multi_spaces_pattern.matcher(userCommand);
 			userCommand = m.replaceAll(" ");
 			//logMessage(userCommand);
			// userCommand = userCommand.replace("\n", "").replace("\r", "");
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");
		*/
	}

	private static void initializeDatabaseInfo() {
		int pkValue = 1;
		File directory = new File("Database/Catalog");
		if (!directory.exists())
			directory.mkdirs();
		try {
			RandomAccessFile sBTablesTableFile = new RandomAccessFile("Database/Catalog/sb_tables.tbl", "rw");
			Table sBTablesTable = UtilityTools.sbTablesTable;
			sBTablesTableFile.setLength(sBTablesTableFile.length() + UtilityTools.pageSize);
			long initialPagePointer = sBTablesTableFile.getFilePointer();
			sBTablesTableFile.seek(initialPagePointer);
			sBTablesTableFile.writeByte(0x0D);
			long numCellPagesPointer = sBTablesTableFile.getFilePointer();
			sBTablesTableFile.skipBytes(1);
			long startCellContentAreaPointer = sBTablesTableFile.getFilePointer();
			sBTablesTableFile.writeShort(512);
			long rightSiblingPagePointer = sBTablesTableFile.getFilePointer();
			sBTablesTableFile.skipBytes(4);
			long cellLocationsStartPointer = sBTablesTableFile.getFilePointer();
			long endOfHeader = sBTablesTableFile.getFilePointer();
			//Tables table
			HashMap<String, String> firstValues = new HashMap<String,String>(), secondValues = new HashMap<String,String>();
			firstValues.put("row_id", pkValue+"");
			firstValues.put("table_name", "sb_tables");
			firstValues.put("database_name", "catalog");
			firstValues.put("record_count", "2");
			firstValues.put("avg_length", "0");
			int recordsLength = sBTablesTable.getRecordLength(firstValues);
			pkValue++;
			secondValues.put("row_id", pkValue + "");
			secondValues.put("table_name", "sb_tables");
			secondValues.put("database_name", "catalog");
			secondValues.put("record_count", "2");
			secondValues.put("avg_length", "0");
			recordsLength += sBTablesTable.getRecordLength(secondValues);
			firstValues.put("avg_length", (recordsLength / 2) + "");
			SplitPage splitPage = traverseAndInsert(sBTablesTableFile, Integer.parseInt(firstValues.get("row_id")), 0, firstValues, sBTablesTable);
			//Added randomly
			//int recordSize = UtilityTools.sbTablesTable.getRecordLength();
			long endOfPage = initialPagePointer + UtilityTools.pageSize;
			sBTablesTableFile.close();
		} catch (IOException | ParseException e) {
			e.printStackTrace();
		}
	}
	/*
	private static boolean insertIntoPage(RandomAccessFile tableFile, HashMap<String, String> values, Table table, int pageNumberToInsert) {
		if(pageNumberToInsert < 0) {
			logMessage("Invalid page number!");
			return false;
		}
		if(!table.validateValues(values)) {
			logMessage("Record not valid! Please check the record and try again.");
			return false;
		}
		int recordLength = table.getRecordLength(values);
		long initialPagePointer = pageNumberToInsert * UtilityTools.pageSize;
		try {
			tableFile.seek(initialPagePointer);
			byte pageType = tableFile.readByte();
			long numCellPagesPointer = tableFile.getFilePointer();
			tableFile.skipBytes(1);
			long startCellContentAreaPointer = tableFile.getFilePointer();
			short offset = tableFile.readShort();
			long rightSiblingPagePointer = tableFile.getFilePointer();
			tableFile.skipBytes(4);
			long cellLocationsStartPointer = tableFile.getFilePointer();
			long endOfHeader = tableFile.getFilePointer();
			if(pageType != 0X0D) {
				logMessage("Page is not leaf!");
				return false;
			}

		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}
	*/

	public static SplitPage traverseAndInsert(RandomAccessFile tableFile, int primaryKey, int pageNumber, HashMap<String, String> values, Table table) throws IOException, ParseException {
		PageHeader pageHeader = new PageHeader(tableFile, pageNumber);
		List<Short> cellLocations = pageHeader.getCellLocations();
		if(pageHeader.getPageType() == 0x05) {
			logMessage("Brood: 1");
			int left = 0, right = pageHeader.getNumCells() - 1, mid;
			DataCellInterior dataCellInterior = new DataCellInterior(tableFile, pageHeader.getPageStartFP() + cellLocations.get(right));
			if(primaryKey > dataCellInterior.getKey()) {
				SplitPage splitPage = traverseAndInsert(tableFile, primaryKey, pageHeader.getRightChiSibPointer(), values, table);
				if(splitPage.shouldSplit) {
					logMessage("Brood: 2");
					return new SplitPage(true);
					//Check if space is available otherwise split.
				}
			} else if(primaryKey == dataCellInterior.getKey()) {
				SplitPage splitPage = traverseAndInsert(tableFile, primaryKey, dataCellInterior.getLeftChildPointer(), values, table);
				if(splitPage.shouldSplit) {
					logMessage("Brood: 3");
					//Check if space is available otherwise split.
					return new SplitPage(true);
				}
			}
			while(left != right) {
				mid = ((left + right) / 2);
				dataCellInterior = new DataCellInterior(tableFile, cellLocations.get(mid));
				if(primaryKey < dataCellInterior.getKey())
					right = mid;
				else if(primaryKey > dataCellInterior.getKey())
					left = mid + 1;
				else
					break;
			}
			logMessage("Brood: 4");
			if(left == right) {
				SplitPage splitPage = traverseAndInsert(tableFile, primaryKey, pageHeader.getRightChiSibPointer(), values, table);
				logMessage("Brood: 5");
				if(splitPage.shouldSplit) {
					//Check if space is available otherwise split.
					return new SplitPage(true);
				} else {
					return splitPage;
				}
			} else {
				logMessage("Primary key must be unique");
				return new SplitPage(false);
			}

		} else if (pageHeader.getPageType() == 0x0D) {
			logMessage("Brood: 6");
			short space = (short) (pageHeader.getCellContentStartOffset() - pageHeader.getHeaderEndOffset());
			short recordLength = table.getRecordLength(values);
			if(recordLength + 2 < space) {
				logMessage("Brood: 7");
				short locationOffset = (short) (pageHeader.getCellContentStartOffset() - recordLength);
				short left = 0, right = (short) (pageHeader.getNumCells() - 1), mid;
				DataCellInterior dataCellInterior = new DataCellInterior(tableFile, cellLocations.get(right));
				if(dataCellInterior.getKey() < primaryKey) {
					tableFile.seek(pageHeader.getHeaderEndOffset());
					tableFile.writeShort(locationOffset);
				} else if(dataCellInterior.getKey() == primaryKey) {
					SplitPage splitPage = new SplitPage(false);
					logMessage("Primary key must be unique");
					return splitPage;
				}
				while(left != right) {
					mid = (short) ((left + right) / 2);
					dataCellInterior = new DataCellInterior(tableFile, cellLocations.get(mid));
					if(primaryKey < dataCellInterior.getKey())
						right = mid;
					else if(primaryKey > dataCellInterior.getKey())
						left = (short) (mid + 1);
					else
						break;
				}
				if (left == right) {
					long leftLocFP = pageHeader.getPageStartFP() + 8 + 2 * left;
					tableFile.seek(leftLocFP);
					byte[] bytes = new byte[2 * (pageHeader.getNumCells() - left)];
					tableFile.read(bytes, 0, 2 * (left - pageHeader.getNumCells()));
					tableFile.seek(leftLocFP);
					tableFile.writeShort(locationOffset);
					tableFile.write(bytes);
					tableFile.seek(pageHeader.getPageStartFP() + 1);
					tableFile.writeByte(pageHeader.getNumCells() + 1);
				} else {
					SplitPage splitPage = new SplitPage(false);
					logMessage("Primary key must be unique");
					return splitPage;
				}
				long recordStart = pageHeader.getPageStartFP() + locationOffset;
				table.writeRecord(tableFile, values, recordStart);
				logMessage("Inserted!");
				return new SplitPage(true);
			} else {
				logMessage("Brood: 8");
				return new SplitPage(true);
				//Split
			}
		} else {
			logMessage("Incorrect Page type");
			return new SplitPage(false);
		}
	}

	private static int traverseTillFound(RandomAccessFile tableFile, int primaryKey) throws IOException {
		int pageNumber = 0, cellOffset, leftPageNumber, cellPk, beforePageNumber;
		byte pageType, numCells;
		long initialPagePointer, numCellPagesPointer, startCellContentAreaPointer, rightSiblingPagePointer, cellLocationsStartPointer;
		while(true) {
			beforePageNumber = pageNumber;
			tableFile.seek(pageNumber * UtilityTools.pageSize);
			initialPagePointer = tableFile.getFilePointer();
			pageType = tableFile.readByte();
			numCellPagesPointer = tableFile.getFilePointer();
			numCells = tableFile.readByte();
			if (pageType == 0x0d) {
				return pageNumber;
			} else if(numCells == 0) {
				logMessage("Something went wrong with the file. Its non-leaf page is empty.");
				return -1;
			} else if(pageType != 0x05) {
				logMessage("Something went wrong with the traverse, currently in a not a table page.");
				return -1;
			}
			startCellContentAreaPointer = tableFile.getFilePointer();
			tableFile.skipBytes(2);
			rightSiblingPagePointer = tableFile.getFilePointer();
			tableFile.skipBytes(4);
			cellLocationsStartPointer = tableFile.getFilePointer();
			int left = 0, mid;
			int right = numCells - 1;
			tableFile.seek(cellLocationsStartPointer + right * 2);
			cellOffset= tableFile.readShort();
			tableFile.seek(initialPagePointer + cellOffset);
			leftPageNumber = tableFile.readInt();
			cellPk = tableFile.readInt();
			if(primaryKey > cellPk) {
				tableFile.seek(rightSiblingPagePointer);
				pageNumber = tableFile.readInt();
			} else if(primaryKey == cellPk) {
				pageNumber = leftPageNumber;
			} else {
				while(left != right) {
					mid = (left + right) / 2;
					tableFile.seek(cellLocationsStartPointer + mid * 2);
					cellOffset= tableFile.readShort();
					tableFile.seek(initialPagePointer + cellOffset);
					leftPageNumber = tableFile.readInt();
					cellPk = tableFile.readInt();
					if(primaryKey < cellPk)
						right = mid;
					else if(primaryKey > cellPk)
						left = mid + 1;
					else
						break;
				}
				if(left == right) {
					tableFile.seek(cellLocationsStartPointer + left * 2);
					cellOffset= tableFile.readShort();
					tableFile.seek(initialPagePointer + cellOffset);
					leftPageNumber = tableFile.readInt();
				}
				pageNumber = leftPageNumber;
			}
			if(pageNumber == beforePageNumber) {
				logMessage("Infinite loop! Check code or database file!");
				return -1;
			}
		}
	}

	private static void splashScreen() {
		System.out.println(line("-", 80));
		System.out.println("Welcome to ScyllaBase");
		System.out.println("ScyllaBase Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-", 80));
	}

	public static String line(String s, int num) {
		StringBuilder a = new StringBuilder();
		for (int i = 0; i < num; i++) {
			a.append(s);
		}
		return a.toString();
	}

	public static void help() {
		System.out.println(line("*", 80));
		System.out.println("SUPPORTED COMMANDS");
		System.out.println("All commands below are case insensitive");
		System.out.println();
		System.out.println("\tSELECT * FROM table_name;                        Display all records in the table.");
		System.out.println("\tSELECT * FROM table_name WHERE rowid = <value>;  Display records whose rowid is <id>.");
		System.out.println("\tDROP TABLE table_name;                           Remove table data and its schema.");
		System.out.println("\tVERSION;                                         Show the program version.");
		System.out.println("\tHELP;                                            Show this help information");
		System.out.println("\tEXIT;                                            Exit the program");
		System.out.println();
		System.out.println();
		System.out.println(line("*", 80));
	}

	public static String getVersion() {
		return UtilityTools.version;
	}

	public static String getCopyright() {
		return UtilityTools.copyright;
	}

	public static void displayVersion() {
		System.out.println("ScyllaBase Version " + getVersion());
		System.out.println(getCopyright());
	}

	public static void parseUserCommand(String userCommand) {

		// String[] commandTokens = userCommand.split(" ");
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

		switch (commandTokens.get(0)) {
			case "select":
				logMessage("Select Called");
				//parseQueryString(userCommand);
				break;
			case "drop":
				logMessage("Drop Called");
				//dropTable(userCommand);
				break;
			case "create":
				if (commandTokens.get(1).equals("database")) {
					logMessage("Create database Called");
				} else if (commandTokens.get(1).equals("table")) {
					logMessage("Create table Called");
				} else {
					logMessage("Wrong type of create called");
				}
				//parseCreateString(userCommand);
				break;
			case "help":
				help();
				break;
			case "version":
				displayVersion();
				break;
			case "exit":
				isExit = true;
				break;
			case "quit":
				isExit = true;
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				break;
		}
	}

	public static void dropTable(String dropTableString) {
		System.out.println("STUB: Calling parseQueryString(String s) to process queries");
		System.out.println("Parsing the string:\"" + dropTableString + "\"");
	}

	public static void parseQueryString(String queryString) {
		System.out.println("STUB: Calling parseQueryString(String s) to process queries");
		System.out.println("Parsing the string:\"" + queryString + "\"");
	}

	public static void parseCreateString(String createTableString) {

		logMessage("Calling create on the query\n" + createTableString);
		Pattern createTablePattern = Pattern.compile("create table ([^(]*)\\((.*)\\)");
		Matcher commandMatcher = createTablePattern.matcher(createTableString);
		logMessage(commandMatcher.group(1));
		String tableName = commandMatcher.group(1).trim();
		//String column_string = commandMatcher.group(2).trim();
		String insert_query = "Insert INTO parts values";
		logMessage("Calling create on the query\n" + createTableString);
		/*
		Pattern createTablePattern = Pattern.compile("create table ([^(]*)\\((.*)\\)");
		Matcher commandMatcher = createTablePattern.matcher(createTableString);
		*/
		String tableFileName = tableName + ".tbl";

		/* YOUR CODE GOES HERE */
		/*  Code to create a .tbl file to contain table data */
		try {
			RandomAccessFile tableFile = new RandomAccessFile(tableFileName, "rw");
			/*Table Header*/
			tableFile.setLength(UtilityTools.pageSize);
			tableFile.writeByte(0x0D);
			//tableFile.writeByte(0x00);
			//tableFile.writeByte(0x00);
			//tableFile.writeByte(0x00);
			logMessage(tableFile.getFilePointer() + "");
			tableFile.close();
			logMessage("File created");
		} catch (Exception e) {
			e.printStackTrace();
		}

		/*  Code to insert a row in the davisbase_tables table
		 *  i.e. database catalog meta-data
		 */

		/*  Code to insert rows in the davisbase_columns table
		 *  for each column in the new table
		 *  i.e. database catalog meta-data
		 */
	}

	private static void logMessage(String message) {
		System.out.println(message);
	}
}