package com.scyllabase;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.scyllabase.UtilityTools.sbColumnsTable;
import static com.scyllabase.UtilityTools.sbTablesTable;

public class ScyllaBase {

	private static boolean isExit = false;
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	private static String currentDb = null;

	public static void main(String[] args) {
		splashScreen();
		initializeDatabaseInfo();
		parseCreateDatabaseString("create database industry");
		parseShowDatabasesQuery();
		parseUseDatabaseString("use industry");
		parseCreateString("create table parts (description TEXT, availability INT NOT NULL)");
		parseSelectString("select * from parts");
		parseInsertString("insert into parts(availability) values(0)");
		/*
		parseInsertString("insert into parts(availability) values(1)");
		parseInsertString("insert into parts(availability) values(0)");
		parseInsertString("insert into parts(availability) values(0)");
		parseInsertString("insert into parts(availability) values(2)");
		parseInsertString("insert into parts(availability) values(0)");
		parseInsertString("insert into parts(availability) values(0)");
		parseInsertString("insert into parts(availability) values(4)");
		parseInsertString("insert into parts(availability) values(0)");
		parseInsertString("insert into parts(availability) values(6)");
		parseInsertString("insert into parts(availability) values(0)");
		parseInsertString("insert into parts(availability) values(0)");
		parseInsertString("insert into parts(availability) values(7)");
		parseInsertString("insert into parts(availability) values(0)");
		parseInsertString("insert into parts(availability) values(0)");
		parseInsertString("insert into parts(availability) values(3)");
		parseInsertString("insert into parts(availability) values(0)");
		parseInsertString("insert into parts(availability) values(0)");
		parseInsertString("insert into parts(availability) values(0)");
		parseInsertString("insert into parts(availability) values(0)");
		parseInsertString("insert into parts(availability) values(6)");
		parseInsertString("insert into parts(availability) values(0)");
		parseInsertString("insert into parts(availability) values(4)");
		parseInsertString("insert into parts(availability) values(0)");
		*/
		parseSelectString("select * from parts");
		response("update parts set availability = 1 where availability = 0");
		parseUpdateString("update parts set availability = 1 where availability = 0");
		parseSelectString("select * from parts");
		parseDeleteString("delete from parts where availability = 1");
		parseSelectString("select * from parts");
		parseDropString("drop table parts");
		parseDropDatabaseString("drop database industry");

		//showTables();
		//showColumns();

		/*
		String userCommand;
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

	private static void showTables() {
		try {
			RandomAccessFile sBTablesTableFile = new RandomAccessFile("Database/catalog/sb_tables.tbl", "rw");
			List<Column> selectColumns = new ArrayList<>();
			for (Map.Entry<String, Column> entry: sbTablesTable.getColumns().entrySet()) {
				selectColumns.add(entry.getValue());
			}
			displayTableHeader(selectColumns);
			traverseAndSelect(sBTablesTableFile, 0, selectColumns, sbTablesTable, null, true);
			System.out.println();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void displayTableHeader(List<Column> selectColumns) {
		System.out.println("-----------------------------------------------------------------------------------------------------------------------------------------");
		for (Column column : selectColumns) {
			System.out.print("|\t");
			System.out.format(column.getFormat(), column.getName());
			System.out.print("\t|");
		}
		System.out.println();
		System.out.println("-----------------------------------------------------------------------------------------------------------------------------------------");
	}

	private static void showColumns() {
		try {
			RandomAccessFile sBTablesTableFile = new RandomAccessFile("Database/catalog/sb_columns.tbl", "rw");
			List<Column> selectColumns = new ArrayList<>();
			for (Map.Entry<String, Column> entry: sbColumnsTable.getColumns().entrySet()) {
				selectColumns.add(entry.getValue());
			}
			displayTableHeader(selectColumns);
			traverseAndSelect(sBTablesTableFile, 0, selectColumns, sbColumnsTable, null, true);
			System.out.println();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void initializeDatabaseInfo() {
		int pkValue = 1;
		File directory = new File("Database/catalog");
		if (!directory.exists())
			directory.mkdirs();
		try {
			RandomAccessFile sBTablesTableFile = new RandomAccessFile("Database/catalog/sb_tables.tbl", "rw");
			Table sBTablesTable = sbTablesTable;
			if(sBTablesTableFile.length() == 0) {
				logMessage("Tables table inserting");
				sBTablesTableFile.setLength(sBTablesTableFile.length() + UtilityTools.pageSize);
				long initialPagePointer = sBTablesTableFile.getFilePointer();
				sBTablesTableFile.seek(initialPagePointer);
				sBTablesTableFile.writeByte(0x0D);
				sBTablesTableFile.skipBytes(1);
				sBTablesTableFile.writeShort((int) UtilityTools.pageSize);
				sBTablesTableFile.writeInt(-1);
				//Tables table
				LinkedHashMap<String, String> firstValues = new LinkedHashMap<>(), secondValues = new LinkedHashMap<>();
				firstValues.put("row_id", pkValue + "");
				firstValues.put("table_name", "sb_tables");
				firstValues.put("database_name", "catalog");
				firstValues.put("record_count", "2");
				firstValues.put("avg_length", "0");
				int recordsLength = sBTablesTable.getRecordLength(firstValues);
				pkValue++;
				secondValues.put("row_id", pkValue + "");
				secondValues.put("table_name", "sb_columns");
				secondValues.put("database_name", "catalog");
				secondValues.put("record_count", "11");
				secondValues.put("avg_length", "0");
				recordsLength += sBTablesTable.getRecordLength(secondValues);
				firstValues.put("avg_length", (recordsLength / 2) + "");
				if (!sBTablesTable.validateValues(firstValues)) {
					logMessage("First values not valid");
					return;
				}
				traverseAndInsert(sBTablesTableFile, Integer.parseInt(firstValues.get("row_id")), 0, firstValues, sBTablesTable, null);
				if (!sBTablesTable.validateValues(secondValues)) {
					logMessage("Second values not valid");
					return;
				}
				traverseAndInsert(sBTablesTableFile, Integer.parseInt(secondValues.get("row_id")), 0, secondValues, sBTablesTable, null);
			}
			sBTablesTableFile.close();
			RandomAccessFile sBColumnsTableFile = new RandomAccessFile("Database/catalog/sb_columns.tbl", "rw");
			Table sbColumnsTable = UtilityTools.sbColumnsTable;
			if(sBColumnsTableFile.length() == 0) {
				logMessage("Columns table inserting");
				sBColumnsTableFile.setLength(sBColumnsTableFile.length() + UtilityTools.pageSize);
				long initialPagePointer = sBColumnsTableFile.getFilePointer();
				sBColumnsTableFile.seek(initialPagePointer);
				sBColumnsTableFile.writeByte(0x0D);
				sBColumnsTableFile.skipBytes(1);
				sBColumnsTableFile.writeShort((int) UtilityTools.pageSize);
				sBColumnsTableFile.writeInt(-1);
				for (LinkedHashMap<String, String> values : UtilityTools.getSbColumnsTableValues()) {
					if (!sbColumnsTable.validateValues(values)) {
						logMessage("First values not valid");
						return;
					}
					traverseAndInsert(sBColumnsTableFile, Integer.parseInt(values.get("row_id")), 0, values, sbColumnsTable, null);
				}
			}
			/*
			//Testing
			List<Column> columnstableColumns = new ArrayList<>();
			for(Map.Entry<String, Column> entry : sbColumnsTable.getColumns().entrySet()) {
				Column column = entry.getValue();
				columnstableColumns.add(column);
			}
			List<Column> columnsss = traverseAndGetColumns(sBColumnsTableFile, 0, sbColumnsTable, new Condition(sbColumnsTable.getColumns().get("table_name"), "=", new TextType("sb_tables")));
			for (Column columnss: columnsss) {
				logMessage(columnss.getName() + " " +columnss.getType() + columnss.getOrdinalPosition() + columnss.getTableName());
			}
			*/

			sBColumnsTableFile.close();
		} catch (IOException | ParseException e) {
			e.printStackTrace();
		}
	}

	private static boolean atomicTraverseAndUpdate(RandomAccessFile file, Table table, Condition condition, LinkedHashMap<String, String> values) throws IOException, ParseException {
		file.seek(0);
		long oldFileLength = file.length();
		byte[] bytes = new byte[(int) oldFileLength];
		file.readFully(bytes);
		UpdateResult updateResult = traverseAndUpdate(file, 0, table, condition, values, -1, -1, new ArrayList<>());
		if(updateResult.isIntegrityViolated()) {
			file.seek(0);
			file.setLength(oldFileLength);
			file.write(bytes);
			return true;
		} else
			return false;
	}

	private static UpdateResult traverseAndUpdate(RandomAccessFile file, int pageNumber, Table table, Condition condition, LinkedHashMap<String, String> values, int lui, int lupk, List<Integer> insertedList) throws IOException, ParseException {
		//TODO Make it execute atomic.
		PageHeader pageHeader = new PageHeader(file, pageNumber);
		UpdateResult updateResult = new UpdateResult();
		if(condition != null && condition.column.isPk() && condition.operation.equals("is null"))
			return updateResult;
		if(condition != null && condition.column.isPk() && condition.operation.equals("is not null"))
			condition = null;
		if(pageHeader.getNumCells() == 0)
			return updateResult;

		if(pageHeader.getPageType() == 0x05) {
			//Current node is table interior
			return traverseAndUpdateInterior(file, pageNumber, pageHeader, table, condition, values, lui, lupk, insertedList);
		} else if(pageHeader.getPageType() == 0x0D) {
			//Current node is table Leaf
			return traverseAndUpdateLeaf(file, pageNumber, pageHeader, table, condition, values, lui, lupk, insertedList);
		} else {
			logMessage("Invalid Page");
			return updateResult;
		}
	}

	private static SplitPage updateRecord(RandomAccessFile file, int searchKey, int pageNumber, PageHeader pageHeader, Table table, LinkedHashMap<String, String> values) throws IOException, ParseException {
		long pageStartPointer = pageHeader.getPageStartFP();
		file.seek(pageStartPointer + pageHeader.getCellLocations().get(searchKey));
		int oldRecordLength = file.readShort() + 6;
		file.skipBytes(4);
		int newRecordLength = 6;
		LinkedHashMap<String, Column> columns = table.getColumns();
		Byte numColumns = file.readByte();
		if(numColumns < columns.entrySet().size()) {
			logMessage("Num of columns in file does not match with num of columns in catalog.");
			return new SplitPage(-1);
		}
		for(Map.Entry<String, Column> entry : columns.entrySet()) {
			String key = entry.getKey();
			Column column = entry.getValue();
			byte dataType = file.readByte();
			if(!column.isPk()) {
				if (!column.isDataTypeCorrect(dataType)) {
					logMessage(dataType + "");
					logMessage(column.getName());
					logMessage(column.getType());
					displayError("Datatype is not correct.");
					return new SplitPage(-1);
				}
				if(column.getType().equals("TEXT") && values.get(column.getName()) != null)
					newRecordLength += values.get(column.getName()).length() + 1;
				else
					newRecordLength += UtilityTools.getNumberOfBytesFromTypebyte(dataType) + 1;
				file.skipBytes(UtilityTools.getNumberOfBytesFromTypebyte(dataType));
			}
		}
		ByteBuffer byteBuffer = ByteBuffer.allocate(newRecordLength);
		byteBuffer.putShort((short) (newRecordLength - 6));
		boolean needToDeleteAndUpdate = values.get(table.getPkColumn().getName()) != null;
		file.seek(pageStartPointer + pageHeader.getCellLocations().get(searchKey) + 2);
		int pkValue = file.readInt();
		int newPkValue = needToDeleteAndUpdate ? Integer.parseInt(values.get(table.getPkColumn().getName())) : pkValue;
		byteBuffer.putInt(newPkValue);
		byteBuffer.put(file.readByte());
		for(Map.Entry<String, Column> entry : columns.entrySet()) {
			String key = entry.getKey();
			Column column = entry.getValue();
			byte dataType = file.readByte();
			if(!column.isPk()) {
				if (!column.isDataTypeCorrect(dataType)) {
					logMessage("Datatype is not correct.");
					return new SplitPage(-1);
				}
				String valuesColumn = values.get(column.getName());
				if(valuesColumn != null) {
					byteBuffer.put(column.getColumnValue(valuesColumn).getByteValue());
				} else {
					int dataTypeLength = UtilityTools.getNumberOfBytesFromTypebyte(dataType);
					byte[] bytes = new byte[dataTypeLength];
					file.read(bytes);
					byteBuffer.put(bytes);
				}
			}
		}
		byte[] newRecord = byteBuffer.array();
		List<Short> cellLocations = pageHeader.getCellLocations();
		if(needToDeleteAndUpdate) {
			//Delete the old record
			traverseAndDelete(file, 0, table, new Condition(table.getPkColumn(), "=", new IntType(pkValue)));
			//deleteRecord(file, searchKey, pageHeader);
			//Insert the new record
			SplitPage insertionSplitPage = traverseAndInsert(file, newPkValue, 0, null, table, newRecord);
			return new SplitPage(newPkValue, insertionSplitPage.isInserted());
		} else {
			if(newRecordLength == oldRecordLength) {
				//If record length is the same just rewrite the record with the new one.
				file.seek(pageStartPointer + pageHeader.getCellLocations().get(searchKey));
				file.write(newRecord);
				return new SplitPage(newPkValue, false);
			} else if(newRecordLength < oldRecordLength) {
				//If record length is the less than the old record length add the record at the start after pushing the other records.
				short newLocation = (short) (pageHeader.getCellLocations().get(searchKey) + newRecordLength - oldRecordLength);
				file.seek(pageStartPointer + newLocation);
				file.write(newRecord);
				file.seek(pageStartPointer + 8);
				int index = 0;
				for (Short cellLocation : cellLocations) {
					if(cellLocation <= cellLocations.get(searchKey)) {
						file.seek(pageStartPointer + 8 + 2 * index);
						file.writeShort(cellLocation + newRecordLength - oldRecordLength);
					}
					index++;
				}
				file.seek(pageStartPointer + 2);
				file.writeShort(pageHeader.getCellContentStartOffset() + newRecordLength - oldRecordLength);
				return new SplitPage(newPkValue, false);
			} else {
				//Check if enough space is available.
				int availableSpace = pageHeader.getCellContentStartOffset() - pageHeader.getHeaderEndOffset();
				if(newRecordLength - oldRecordLength <= availableSpace) {
					//space is available.
					byte[] tempBytes = new byte[pageHeader.getCellLocations().get(searchKey) - pageHeader.getCellContentStartOffset()];
					file.seek(pageStartPointer + pageHeader.getCellContentStartOffset());
					file.read(tempBytes);
					file.seek(pageStartPointer + pageHeader.getCellContentStartOffset() + oldRecordLength);
					file.write(tempBytes);
					file.seek(pageStartPointer + pageHeader.getCellContentStartOffset() + oldRecordLength - newRecordLength);
					file.write(newRecord);
					file.seek(pageStartPointer + 8 + 2 * searchKey);
					file.writeShort(pageHeader.getCellContentStartOffset() + oldRecordLength - newRecordLength);
					file.seek(pageStartPointer + 2);
					file.writeShort(pageHeader.getCellContentStartOffset() + oldRecordLength - newRecordLength);
					return new SplitPage(newPkValue, false);
				} else {
					//Space is not available. Split the page.
					//Delete and insert the record.
					//deleteRecord(file, searchKey, pageHeader);
					traverseAndDelete(file, 0, table, new Condition(table.getPkColumn(), "=", new IntType(pkValue)));
					SplitPage insertionSplitPage = traverseAndInsert(file, newPkValue, 0, null, table, newRecord);
					return new SplitPage(newPkValue, insertionSplitPage.isInserted());
				}
			}
		}
	}

	private static UpdateResult traverseAndUpdateInterior(RandomAccessFile file, int pageNumber, PageHeader pageHeader, Table table, Condition condition, LinkedHashMap<String, String> values, int lui, int lupk, List<Integer> insertedPks) throws IOException, ParseException {
		UpdateResult updateResult = new UpdateResult();
		//Page is not leaf
		updateResult.setLeaf(false);
		long pageStartPointer = pageHeader.getPageStartFP();
		List<Integer> insertedPksTemp = new ArrayList<>();
		insertedPksTemp.addAll(insertedPks);
		updateResult.setNewPksInserted(new ArrayList<>());
		UpdateResult subUpdateResult;
		int index;
		List<Short> cellLocations = pageHeader.getCellLocations();
		if(condition != null && condition.column.isPk() && (condition.operation.equals(">") || condition.operation.equals(">=") || condition.operation.equals("=") || condition.operation.equals("<") || condition.operation.equals("<="))) {
			if(condition.operation.equals("=")) {
				boolean integrityViolated = false;
				int lastUpdatedPk = lupk;
				index = smallestKeyGreaterEqual(file, pageHeader, condition, false);
				if(index == -1) {
					subUpdateResult = traverseAndUpdate(file, pageHeader.getRightChiSibPointer(), table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
				} else {
					subUpdateResult = traverseAndUpdate(file, new DataCellPage(file, cellLocations.get(index), false).getLeftChildPointer(), table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
				}
				integrityViolated = subUpdateResult.isIntegrityViolated();
				updateResult.setIntegrityViolated(integrityViolated);
				insertedPksTemp.addAll(subUpdateResult.getNewPksInserted());
				updateResult.addNewPksInserted(subUpdateResult.getNewPksInserted());
			} else if(condition.operation.equals(">=") || condition.operation.equals(">")) {
				boolean integrityViolated = false, pageCompletelyUpdated = false;
				int lastUpdatedPk = lupk;
				while(!integrityViolated && !pageCompletelyUpdated) {
					if(lastUpdatedPk == -1) {
						index = smallestKeyGreaterEqual(file, pageHeader, condition, false);
						if(condition.operation.equals(">")) {
							DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(index), true);
							if(condition.value.equal(dataCellPage.getKey())) {
								index++;
								if(index >= pageHeader.getNumCells())
									index = -1;
							}
						}
					} else {
						index = getNextInteriorUpdateIndex(file, pageHeader, lastUpdatedPk);
					}
					if(index == -1) {
						pageCompletelyUpdated = true;
						subUpdateResult = traverseAndUpdate(file, pageHeader.getRightChiSibPointer(), table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
					} else {
						subUpdateResult = traverseAndUpdate(file, new DataCellPage(file, cellLocations.get(index), false).getLeftChildPointer(), table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
					}
					//UpdateNextResult updateNextResult = traverseAndUpdateOnLastUpdatedInterior(file, lastUpdatedPk, lastUpdatedIndex, true, pageHeader, pageNumber, insertedPks, table, condition, values);
					insertedPksTemp.addAll(subUpdateResult.getNewPksInserted());
					updateResult.addNewPksInserted(subUpdateResult.getNewPksInserted());
					integrityViolated = subUpdateResult.isIntegrityViolated();
					lastUpdatedPk = subUpdateResult.getLastUpdatedPk();
					pageHeader = new PageHeader(file, pageNumber);
				}
				updateResult.setIntegrityViolated(integrityViolated);
			} else {
				//Operation is < or <=
				boolean integrityViolated = false, pageCompletelyUpdated = false, conditionFailed = false;
				int lastUpdatedPk = lupk;
				while(!integrityViolated && !pageCompletelyUpdated && !conditionFailed) {
					if(lastUpdatedPk == -1) {
						index = 0;
					} else {
						index = getNextInteriorUpdateIndex(file, pageHeader, lastUpdatedPk);
					}
					if(index == -1) {
						pageCompletelyUpdated = true;
						subUpdateResult = traverseAndUpdate(file, pageHeader.getRightChiSibPointer(), table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
					} else {
						subUpdateResult = traverseAndUpdate(file, new DataCellPage(file, cellLocations.get(index), false).getLeftChildPointer(), table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
					}
					//UpdateNextResult updateNextResult = traverseAndUpdateOnLastUpdatedInterior(file, lastUpdatedPk, lastUpdatedIndex, true, pageHeader, pageNumber, insertedPks, table, condition, values);
					insertedPksTemp.addAll(subUpdateResult.getNewPksInserted());
					updateResult.addNewPksInserted(subUpdateResult.getNewPksInserted());
					integrityViolated = subUpdateResult.isIntegrityViolated();
					conditionFailed = subUpdateResult.isConditionFailed();
					lastUpdatedPk = subUpdateResult.getLastUpdatedPk();
					pageHeader = new PageHeader(file, pageNumber);
				}
				updateResult.setIntegrityViolated(integrityViolated);
				updateResult.setConditionFailed(conditionFailed);
			}
		} else {
			boolean integrityViolated = false, pageCompletelyUpdated = false;
			int lastUpdatedPk = lupk;
			while(!integrityViolated && !pageCompletelyUpdated) {
				index = getNextInteriorUpdateIndex(file, pageHeader, lastUpdatedPk);
				if(index == -1) {
					pageCompletelyUpdated = true;
					subUpdateResult = traverseAndUpdate(file, pageHeader.getRightChiSibPointer(), table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
				} else {
					subUpdateResult = traverseAndUpdate(file, new DataCellPage(file, cellLocations.get(index), false).getLeftChildPointer(), table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
				}
				//UpdateNextResult updateNextResult = traverseAndUpdateOnLastUpdatedInterior(file, lastUpdatedPk, lastUpdatedIndex, true, pageHeader, pageNumber, insertedPks, table, condition, values);

				insertedPksTemp.addAll(subUpdateResult.getNewPksInserted());
				updateResult.addNewPksInserted(subUpdateResult.getNewPksInserted());
				integrityViolated = subUpdateResult.isIntegrityViolated();
				lastUpdatedPk = subUpdateResult.getLastUpdatedPk();
				pageHeader = new PageHeader(file, pageNumber);
			}
			updateResult.setIntegrityViolated(integrityViolated);
		}
		return updateResult;
	}

	private static int getNextInteriorUpdateIndex(RandomAccessFile file, PageHeader pageHeader, int lastUpdatedPk) throws IOException {
		//Get the Smallest key greater than the last updated pk.
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartPointer = pageHeader.getPageStartFP();
		int left = 0, right = pageHeader.getNumCells() - 1, mid;
		DataCellPage leftDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(left), false);
		DataCellPage rightDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(right), false);
		if(lastUpdatedPk < leftDataCellPage.getKey())
			return left;
		else if(lastUpdatedPk == leftDataCellPage.getKey())
			return left + 1;
		else if(lastUpdatedPk >= rightDataCellPage.getKey())
			return -1;
		while(left != right) {
			mid = (left + right) / 2;
			DataCellPage midDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(mid), false);
			if(lastUpdatedPk > midDataCellPage.getKey())
				left = mid + 1;
			else if(lastUpdatedPk < midDataCellPage.getKey())
				right = mid;
			else
				return mid + 1;
		}
		return left;
	}

	private static UpdateResult traverseAndUpdateLeaf(RandomAccessFile file, int pageNumber, PageHeader pageHeader, Table table, Condition condition, LinkedHashMap<String, String> values, int lui, int lupk, List<Integer> insertedPks) throws IOException, ParseException {
		long pageStartPointer = pageHeader.getPageStartFP();
		UpdateResult updateResult = new UpdateResult();
		//Page is leaf
		updateResult.setLeaf(true);
		List<Short> cellLocations = pageHeader.getCellLocations();
		List<Integer> insertedPksTemp = new ArrayList<>();
		insertedPksTemp.addAll(insertedPks);
		if(condition != null && condition.column.isPk() && (condition.operation.equals(">") || condition.operation.equals(">=") || condition.operation.equals("=") || condition.operation.equals("<") || condition.operation.equals("<="))) {
			if(condition.operation.equals("=")) {
				int searchKey = binarySearchKey(file, pageHeader, condition);
				if (searchKey == -1)
					return updateResult;
				//Update Record and update page header.
				SplitPage updateSplitPage = updateRecord(file, searchKey, pageNumber, pageHeader, table, values);
				if(updateSplitPage.isInserted()) {
					updateResult.keyChange(((IntType)condition.getValue()).getValue());
					updateResult.keyInserted(updateSplitPage.getKey());
				} else {
					updateResult.setLastUpdatedPk(((IntType)condition.getValue()).getValue());
				}
			} else if(condition.operation.equals("<") || condition.operation.equals("<=")) {
				boolean conditionFailed = false, integrityViolated = false, pageCompletelyUpdated = false, pageChangedToInterior = false;
				int lastUpdatedPk = lupk, lastUpdatedIndex = lui;
				while(!conditionFailed && !integrityViolated && !pageCompletelyUpdated) {
					pageHeader = new PageHeader(file, pageNumber);
					if(pageHeader.getPageType() == 0x05) {
						pageChangedToInterior = true;
						break;
					} else if (pageHeader.getPageType() == 0x00) {
						break;
					}
					UpdateNextResult updateNextResult = traverseAndUpdateOnLastUpdatedLeaf(file, lastUpdatedPk, lastUpdatedIndex, true, pageHeader, pageNumber, insertedPks, table, condition, values);
					conditionFailed = updateNextResult.isConditionFailed();
					integrityViolated = updateNextResult.isIntegrityViolated();
					pageCompletelyUpdated = updateNextResult.getLastUpdatedIndex() == -1;
					lastUpdatedIndex = updateNextResult.getLastUpdatedIndex();
					if(updateNextResult.getNewInsertedPk() != -1) {
						updateResult.keyChange(updateNextResult.getLastUpdatedPk());
						updateResult.keyInserted(updateNextResult.getNewInsertedPk());
						insertedPksTemp.add(updateNextResult.getNewInsertedPk());
					} else {
						lastUpdatedPk = updateNextResult.getLastUpdatedPk();
					}
				}
				if(pageChangedToInterior) {
					UpdateResult updateResult1 = traverseAndUpdate(file, pageNumber, table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
					conditionFailed = updateResult1.isConditionFailed();
					integrityViolated = updateResult1.isIntegrityViolated();
					lastUpdatedPk = updateResult1.getLastUpdatedPk();
					updateResult.addNewPksInserted(updateResult1.getNewPksInserted());
				}
				updateResult.setLastUpdatedPk(lastUpdatedPk);
				updateResult.setIntegrityViolated(integrityViolated);
				updateResult.setConditionFailed(conditionFailed);
			} else {
				if(lupk == -1) {
					lui = smallestKeyGreaterEqual(file, pageHeader, condition, true);
					if (lui == -1) {
						logMessage("Something went wrong. skGE is -1. Should not be in this page.");
						updateResult.setIntegrityViolated(true);
						return updateResult;
					}
					if (condition.operation.equals(">")) {
						DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(lui), true);
						if (condition.value.equal(dataCellPage.getKey()))
							lui++;
					}
					lui--;
					if(lui == -1) {
						lupk = -1;
					} else {
						DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(lui), true);
						lupk = dataCellPage.getKey();
					}
				}

				boolean integrityViolated = false, pageCompletelyUpdated = false, pageChangedToInterior = false;
				int lastUpdatedPk = lupk, lastUpdatedIndex = lui;
				while(!integrityViolated && !pageCompletelyUpdated) {
					if(pageHeader.getPageType() == 0x05) {
						pageChangedToInterior = true;
						break;
					} else if (pageHeader.getPageType() == 0x00) {
						break;
					}
					UpdateNextResult updateNextResult = traverseAndUpdateOnLastUpdatedLeaf(file, lastUpdatedPk, lastUpdatedIndex, false, pageHeader, pageNumber, insertedPks, table, condition, values);
					pageHeader = new PageHeader(file, pageNumber);
					integrityViolated = updateNextResult.isIntegrityViolated();
					pageCompletelyUpdated = updateNextResult.getLastUpdatedIndex() == -1;
					lastUpdatedIndex = updateNextResult.getLastUpdatedIndex();
					if(updateNextResult.getNewInsertedPk() != -1) {
						updateResult.keyChange(updateNextResult.getLastUpdatedPk());
						updateResult.keyInserted(updateNextResult.getNewInsertedPk());
						insertedPksTemp.add(updateNextResult.getNewInsertedPk());
					} else {
						lastUpdatedPk = updateNextResult.getLastUpdatedPk();
					}
				}
				if(pageChangedToInterior) {
					UpdateResult updateResult1 = traverseAndUpdate(file, pageNumber, table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
					integrityViolated = updateResult1.isIntegrityViolated();
					lastUpdatedPk = updateResult1.getLastUpdatedPk();
					updateResult.addNewPksInserted(updateResult1.getNewPksInserted());
				}
				updateResult.setIntegrityViolated(integrityViolated);
				updateResult.setLastUpdatedPk(lastUpdatedPk);
			}
		} else {
			boolean integrityViolated = false, pageCompletelyUpdated = false, pageChangedToInterior = false;
			int lastUpdatedPk = lupk, lastUpdatedIndex = lui;
			while(!integrityViolated && !pageCompletelyUpdated) {
				if(pageHeader.getPageType() == 0x05) {
					pageChangedToInterior = true;
					break;
				} else if (pageHeader.getPageType() == 0x00) {
					pageCompletelyUpdated = true;
					break;
				}
				UpdateNextResult updateNextResult = traverseAndUpdateOnLastUpdatedLeaf(file, lastUpdatedPk, lastUpdatedIndex, true, pageHeader, pageNumber, insertedPks, table, condition, values);
				pageHeader = new PageHeader(file, pageNumber);
				integrityViolated = updateNextResult.isIntegrityViolated();
				pageCompletelyUpdated = updateNextResult.getLastUpdatedIndex() == -1;
				lastUpdatedIndex = updateNextResult.getLastUpdatedIndex();
				if(updateNextResult.getNewInsertedPk() != -1) {
					updateResult.keyChange(updateNextResult.getLastUpdatedPk());
					updateResult.keyInserted(updateNextResult.getNewInsertedPk());
					insertedPksTemp.add(updateNextResult.getNewInsertedPk());
				} else {
					lastUpdatedPk = updateNextResult.getLastUpdatedPk();
				}
			}
			if(pageChangedToInterior) {
				UpdateResult updateResult1 = traverseAndUpdate(file, pageNumber, table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
				integrityViolated = updateResult1.isIntegrityViolated();
				lastUpdatedPk = updateResult1.getLastUpdatedPk();
				updateResult.addNewPksInserted(updateResult1.getNewPksInserted());
			}
			updateResult.setIntegrityViolated(integrityViolated);
			updateResult.setLastUpdatedPk(lastUpdatedPk);
		}
		return updateResult;
	}

	private static UpdateNextResult traverseAndUpdateOnLastUpdatedLeaf(RandomAccessFile file, int lastUpdatedPk, int lastUpdatedIndex, boolean checkCondition, PageHeader pageHeader, int pageNumber, List<Integer> newlyInsertedPks, Table table, Condition condition, LinkedHashMap<String, String> values) throws IOException, ParseException {
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartPointer = pageHeader.getPageStartFP();
		if(lastUpdatedIndex != -1 && lastUpdatedIndex > pageHeader.getNumCells()) {
			//If last updated index is not -1 and it is greater than the num of cells means that the next key/Old key is in another page is in another page.
			return new UpdateNextResult(lastUpdatedPk, -1);
		} else if(lastUpdatedIndex == -1 && lastUpdatedPk == -1) {
			//If last update index is -1 and last updated pk is -1, then check the condition
			return updateOnIndexLeaf(file, 0, checkCondition, pageHeader, pageNumber, newlyInsertedPks, table, condition, values);
		} else if(lastUpdatedIndex == -1) {
			//get the next index.
			int nextIndex = getNextIndex(file, pageHeader, lastUpdatedPk);
			if(nextIndex != -1 && nextIndex < pageHeader.getNumCells())
				return updateOnIndexLeaf(file, nextIndex + 1, checkCondition, pageHeader, pageNumber, newlyInsertedPks, table, condition, values);
			else
				return new UpdateNextResult(lastUpdatedPk, -1);
		} else if(lastUpdatedPk != -1) {
			DataCellPage currentInLastUpdatedIndex = new DataCellPage(file, pageStartPointer + cellLocations.get(lastUpdatedIndex), true);
			if(currentInLastUpdatedIndex.getKey() == lastUpdatedPk && lastUpdatedIndex + 1 < pageHeader.getNumCells())
				return updateOnIndexLeaf(file, lastUpdatedIndex + 1, checkCondition, pageHeader, pageNumber, newlyInsertedPks, table, condition, values);
			else if(lastUpdatedPk == currentInLastUpdatedIndex.getKey())
				return new UpdateNextResult(-1, -1);
			else if(currentInLastUpdatedIndex.getKey() > lastUpdatedPk)
				return updateOnIndexLeaf(file, lastUpdatedIndex, checkCondition, pageHeader, pageNumber, newlyInsertedPks, table, condition, values);
			else {
				//something is wrong
				logMessage("Something wrong. should not be here. current pk < lupk");
				return new UpdateNextResult(lastUpdatedPk, -1, true);
			}
		} else {
			//something is wrong
			logMessage("Something wrong. should not be here.");
			return new UpdateNextResult(lastUpdatedPk, -1, true);
		}
	}

	private static short getNextIndex(RandomAccessFile file, PageHeader pageHeader, int lastUpdatedPk) throws IOException {
		short left = 0, right = (short) (pageHeader.getNumCells() - 1), mid;
		long pageStartPointer = pageHeader.getPageStartFP();
		List<Short> cellLocations = pageHeader.getCellLocations();
		DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(right), true);
		if(lastUpdatedPk > dataCellPage.getKey())
			return -1;
		else if (lastUpdatedPk == dataCellPage.getKey())
			return (short) (right + 1);

		while(left != right) {
			mid = (short) ((left + right) / 2);
			dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(mid), true);
			if(lastUpdatedPk > dataCellPage.getKey())
				left = (short) (mid + 1);
			else if (lastUpdatedPk < dataCellPage.getKey())
				right = (short) (mid - 1);
			else
				return (short) (mid + 1);
		}
		return -1;
	}

	private static UpdateNextResult updateOnIndexLeaf(RandomAccessFile file, int index, boolean checkCondition, PageHeader pageHeader, int pageNumber, List<Integer> newlyInsertedPks, Table table, Condition condition, LinkedHashMap<String, String> values) throws IOException, ParseException {
		long pageStartPointer = pageHeader.getPageStartFP();
		int columnIndex = -1;
		if(condition != null && !condition.column.isPk()) {
			columnIndex = table.getPkColumn().getOrdinalPosition() < condition.column.getOrdinalPosition() ? condition.column.getOrdinalPosition() - 1 : condition.column.getOrdinalPosition();
		}
		boolean conditionColumnIsPk = condition != null && condition.column.isPk();
		file.seek(pageStartPointer + pageHeader.getCellLocations().get(index));
		file.skipBytes(2);
		int pk = file.readInt();
		if(newlyInsertedPks.contains(pk))
			return new UpdateNextResult(pk, index);
		if(checkCondition && condition != null) {
			if(conditionColumnIsPk) {
				if(!condition.result(new IntType(pk)))
					return new UpdateNextResult(pk, index, false, true);
			} else {
				file.skipBytes(6);
				int numColumns = file.readByte();
				if (columnIndex >= numColumns) {
					logMessage("Something very wrong happened to the database. Number of columns is less.");
					return new UpdateNextResult(pk, index);
				}
				byte type;
				for (int i = 0; i < columnIndex - 1; i++) {
					type = file.readByte();
					file.skipBytes(UtilityTools.getNumberOfBytesFromTypebyte(type));
				}
				type = file.readByte();
				switch (condition.operation) {
					case "is null":
						if (!UtilityTools.valueNull(type) && type != 0x0C)
							return new UpdateNextResult(pk, index, false, true);
						break;
					case "is not null":
						if (UtilityTools.valueNull(type) || type == 0x0C)
							return new UpdateNextResult(pk, index, false, true);
						break;
					default:
						if (UtilityTools.valueNull(type)) {
							return new UpdateNextResult(pk, index, false, true);
						} else {
							int bytesLength = UtilityTools.getNumberOfBytesFromTypebyte(type);
							byte[] bytes = new byte[bytesLength];
							file.read(bytes);
							DataType tableValue = getDataTypeFromByteType(type, bytes);
							if (!condition.result(tableValue))
								return new UpdateNextResult(pk, index, false, true);
						}
						break;
				}

			}
		}
		//Update the record
		SplitPage splitPage = updateRecord(file, index, pageNumber, pageHeader, table, values);
		if(splitPage.isInserted()) {
			//If a new record is inserted send the newly inserted key along with the new LUI and LUPK.
			return new UpdateNextResult(pk, index, splitPage.getKey(), pk);
		} else if(splitPage.getKey() == -1) {
			//If a new record could not be inserted cause of integrity violation, roll back to the original file.
			return new UpdateNextResult(-1, -1, true);
		} else {
			//If a record is updated, just send the new LUI and LUPK.
			return new UpdateNextResult(pk, index);
		}
	}

	private static DeleteResult traverseAndDelete(RandomAccessFile file, int pageNumber, Table table, Condition condition) throws IOException {
		PageHeader pageHeader = new PageHeader(file, pageNumber);
		DeleteResult deleteResult = new DeleteResult();
		if(condition != null && condition.column.isPk() && condition.operation.equals("is null"))
			return deleteResult;
		if(condition != null && condition.column.isPk() && condition.operation.equals("is not null"))
			condition = null;
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartPointer = pageHeader.getPageStartFP();
		if(pageHeader.getNumCells() == 0)
			return deleteResult;
		if(pageHeader.getPageType() == 0x05) {
			//Current node is table interior
			return traverseAndDeleteInterior(file, pageNumber, pageHeader, table, condition);
		} else if(pageHeader.getPageType() == 0x0D) {
			deleteResult.setLeaf(true);
			//Page is leaf
			if(condition != null && condition.column.isPk() && (condition.operation.equals(">") || condition.operation.equals(">=") || condition.operation.equals("=") || condition.operation.equals("<") || condition.operation.equals("<="))) {
				if(condition.operation.equals("=")) {
					int searchKey = binarySearchKey(file, pageHeader, condition);
					if (searchKey == -1)
						return deleteResult;
					//Delete Record and update page header.
					int deletedPk = deleteRecord(file, searchKey, pageHeader);
					deleteResult.deleteKey(deletedPk);
					pageHeader = new PageHeader(file, pageNumber);
					if(pageHeader.getNumCells() == 0 && pageNumber != 0) {
						deleteResult.setWholePageDeleted(true);
						deleteResult.setRightSiblingPageNumber(pageHeader.getRightChiSibPointer());
						file.seek(pageStartPointer);
						file.write(0x00);
					}
					return deleteResult;
				} else if(condition.operation.equals("<") || condition.operation.equals("<=")) {
					int key = largestKeyLesserEqual(file, pageHeader, condition, true);
					if(key == -1) {
						return deleteResult;
					}
					if(condition.operation.equals("<")) {
						DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(key), true);
						if(condition.value.equal(dataCellPage.getKey()))
							key--;
					}
					for (int i = 0; i <= key; i++) {
						//Delete Record and update page header.
						int deletedPk = deleteRecord(file, i - deleteResult.getNumOfRecordsDeleted(), pageHeader);
						deleteResult.deleteKey(deletedPk);
						pageHeader = new PageHeader(file, pageNumber);
					}
					if(pageHeader.getNumCells() == 0 && pageNumber != 0) {
						deleteResult.setWholePageDeleted(true);
						deleteResult.setRightSiblingPageNumber(pageHeader.getRightChiSibPointer());
						deleteResult.setUpdateRightMostChildRightPointer(true);
						file.seek(pageStartPointer);
						file.write(0x00);
					}
					return deleteResult;
				} else {
					int key = smallestKeyGreaterEqual(file, pageHeader, condition, true);
					if(key == -1) {
						logMessage("Something went wrong. skGE is -1. Should not be in this page.");
						return new DeleteResult();
					}
					if(condition.operation.equals(">")) {
						DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(key), true);
						if(condition.value.equal(dataCellPage.getKey()))
							key++;
					}
					for (int i = key; i < cellLocations.size(); i++) {
						int deletedPk = deleteRecord(file, i - deleteResult.getNumOfRecordsDeleted(), pageHeader);
						deleteResult.deleteKey(deletedPk);
						pageHeader = new PageHeader(file, pageNumber);
					}
					if(pageHeader.getNumCells() == 0 && pageNumber != 0) {
						deleteResult.setWholePageDeleted(true);
						deleteResult.setRightSiblingPageNumber(pageHeader.getRightChiSibPointer());
						deleteResult.setUpdateRightMostChildRightPointer(true);
						file.seek(pageStartPointer);
						file.write(0x00);
					}
					return deleteResult;
				}
			} else {
				int columnIndex = -1;
				if(condition != null && !condition.column.isPk()) {
					columnIndex = table.getPkColumn().getOrdinalPosition() < condition.column.getOrdinalPosition() ? condition.column.getOrdinalPosition() - 1 : condition.column.getOrdinalPosition();
				}
				boolean conditionColumnIsPk = condition != null && condition.column.isPk();
				int sizeOfCellLocations = cellLocations.size();
				for (int j = 0; j < sizeOfCellLocations; j++) {
					file.seek(pageStartPointer + cellLocations.get(j - deleteResult.getNumOfRecordsDeleted()));
					if(condition != null) {
						if(conditionColumnIsPk) {
							file.skipBytes(2);
							int pk = file.readInt();
							if(!condition.result(new IntType(pk)))
								continue;
						} else {
							file.skipBytes(6);
							int numColumns = file.readByte();
							if (columnIndex >= numColumns) {
								logMessage("Something very wrong happened to the database. Number of columns is less.");
								return deleteResult;
							}
							byte type;
							for (int i = 0; i < columnIndex - 1; i++) {
								type = file.readByte();
								file.skipBytes(UtilityTools.getNumberOfBytesFromTypebyte(type));
							}
							type = file.readByte();
							switch (condition.operation) {
								case "is null":
									if (!UtilityTools.valueNull(type) && type != 0x0C)
										continue;
									break;
								case "is not null":
									if (UtilityTools.valueNull(type) || type == 0x0C)
										continue;
									break;
								default:
									if (UtilityTools.valueNull(type)) {
										continue;
									} else {
										int bytesLength = UtilityTools.getNumberOfBytesFromTypebyte(type);
										byte[] bytes = new byte[bytesLength];
										file.read(bytes);
										DataType tableValue = getDataTypeFromByteType(type, bytes);
										if (!condition.result(tableValue))
											continue;
									}
									break;
							}
						}
					}
					int deletedPk = deleteRecord(file, j - deleteResult.getNumOfRecordsDeleted(), pageHeader);
					deleteResult.deleteKey(deletedPk);
					pageHeader = new PageHeader(file, pageNumber);
					cellLocations = pageHeader.getCellLocations();
				}
				if(pageHeader.getNumCells() == 0 && pageNumber != 0) {
					deleteResult.setWholePageDeleted(true);
					deleteResult.setRightSiblingPageNumber(pageHeader.getRightChiSibPointer());
					deleteResult.setUpdateRightMostChildRightPointer(true);
					file.seek(pageStartPointer);
					file.write(0x00);
				}
				return deleteResult;
			}
		} else {
			logMessage("Invalid Page");
			return deleteResult;
		}
	}


	private static DeleteResult traverseAndDeleteInterior(RandomAccessFile file, int pageNumber, PageHeader pageHeader, Table table, Condition condition) throws IOException {
		DeleteResult deleteResult = new DeleteResult();
		deleteResult.setLeaf(false);
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartPointer = pageHeader.getPageStartFP();
		if(condition != null && condition.column.isPk() && (condition.operation.equals(">") || condition.operation.equals(">=") || condition.operation.equals("=") || condition.operation.equals("<") || condition.operation.equals("<="))) {
			if(condition.operation.equals("=")) {
				int key = smallestKeyGreaterEqual(file, pageHeader, condition, false);
				int traversePointer = -1;
				DataCellPage dataCellPage = null;
				if(key == -1)
					traversePointer = pageHeader.getRightChiSibPointer();
				else
					dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(key), false);
				DeleteResult subDeleteResult = traverseAndDelete(file, key == -1 ? traversePointer : dataCellPage.getLeftChildPointer(), table, condition);
				deleteResult = updateDeleteAfterTraversal(file, key, pageHeader, pageNumber, deleteResult, subDeleteResult, key == -1 ? -1: dataCellPage.getKey());
				return deleteResult;
			} else if(condition.operation.equals("<") || condition.operation.equals("<=")) {
				int numCellsDeleted = 0;
				int key = largestKeyLesserEqual(file, pageHeader, condition, false);
				if(key == -1) {
					return deleteResult;
				}
				if(condition.operation.equals("<")) {
					DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(key), false);
					if(condition.value.equal(dataCellPage.getKey()))
						key--;
				}
				for (int i = 0; i <= key; i++) {
					//Delete Record and update page header.
					DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(i - numCellsDeleted), false);
					DeleteResult subDeleteResult = traverseAndDelete(file, dataCellPage.getLeftChildPointer(), table, condition);
					deleteResult = updateDeleteAfterTraversal(file, i - numCellsDeleted, pageHeader, pageNumber, deleteResult, subDeleteResult, dataCellPage.getKey());
					if(subDeleteResult.isWholePageDeleted())
						numCellsDeleted++;
					pageHeader = new PageHeader(file, pageNumber);
					cellLocations = pageHeader.getCellLocations();
				}
				return deleteResult;
			} else {
				int numCellsDeleted = 0;
				int key = smallestKeyGreaterEqual(file, pageHeader, condition, false);
				if(key == -1) {
					logMessage("Something went wrong. skGE is -1. Should not be in this page.");
					return new DeleteResult();
				}
				if(condition.operation.equals(">")) {
					DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(key), true);
					if(condition.value.equal(dataCellPage.getKey()))
						key++;
				}
				int cellLocationsSize = cellLocations.size();
				int i;
				for (i = key; i < cellLocationsSize; i++) {
					//Delete Record and update page header.
					DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(i - numCellsDeleted), false);
					DeleteResult subDeleteResult = traverseAndDelete(file, dataCellPage.getLeftChildPointer(), table, condition);
					deleteResult = updateDeleteAfterTraversal(file, i - numCellsDeleted, pageHeader, pageNumber, deleteResult, subDeleteResult, dataCellPage.getKey());
					if(subDeleteResult.isWholePageDeleted())
						numCellsDeleted++;
					pageHeader = new PageHeader(file, pageNumber);
					cellLocations = pageHeader.getCellLocations();
				}
				if(i == cellLocationsSize) {
					DeleteResult subDeleteResult = traverseAndDelete(file, pageHeader.getRightChiSibPointer(), table, condition);
					deleteResult = updateDeleteAfterTraversal(file, -1, pageHeader, pageNumber, deleteResult, subDeleteResult, -1);
				}
				return deleteResult;
			}
		} else {
			int numCellsDeleted = 0;
			int cellLocationsSize = cellLocations.size();
			if(cellLocationsSize == 0) {
				logMessage("Something went wrong. no cells in this page.");
				return new DeleteResult();
			}
			for (int i = 0; i < cellLocationsSize; i++) {
				//Delete Record and update page header.
				DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(i - numCellsDeleted), false);
				DeleteResult subDeleteResult = traverseAndDelete(file, dataCellPage.getLeftChildPointer(), table, condition);
				deleteResult = updateDeleteAfterTraversal(file, i - numCellsDeleted, pageHeader, pageNumber, deleteResult, subDeleteResult, dataCellPage.getKey());
				if(subDeleteResult.isWholePageDeleted())
					numCellsDeleted++;
				pageHeader = new PageHeader(file, pageNumber);
				cellLocations = pageHeader.getCellLocations();
			}
			DeleteResult subDeleteResult = traverseAndDelete(file, pageHeader.getRightChiSibPointer(), table, condition);
			deleteResult = updateDeleteAfterTraversal(file, -1, pageHeader, pageNumber, deleteResult, subDeleteResult, -1);
			return deleteResult;
		}
	}

	private static DeleteResult updateDeleteAfterTraversal(RandomAccessFile file, int key, PageHeader pageHeader, int pageNumber , DeleteResult deleteResult, DeleteResult subDeleteResult, int dcpKey) throws IOException {

		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartPointer = pageHeader.getPageStartFP();
		//If we need to update the right most child pointer and there are keys before, update else send to top. If page is root then dont do anything if there are no keys.
		if(subDeleteResult.isUpdateRightMostChildRightPointer()) {
			if((key == 0 || (key == -1 && pageHeader.getNumCells() == 0)) && pageNumber != 0) {
				deleteResult.setUpdateRightMostChildRightPointer(true);
				deleteResult.setRightSiblingPageNumber(subDeleteResult.getRightSiblingPageNumber());
			} else if(!(key == 0 || (key == -1 && pageHeader.getNumCells() == 0))) {
				DataCellPage dataCellPage1 = new DataCellPage(file, pageStartPointer + cellLocations.get(key == -1 ? cellLocations.size() - 1 : key - 1), false);
				updateRightMostChildRightPointer(file, dataCellPage1.getLeftChildPointer(), subDeleteResult.getRightSiblingPageNumber());
			}
		}

		if(subDeleteResult.getOnePageNumber()!= -1) {
			if(key != -1) {
				file.seek(pageStartPointer + cellLocations.get(key));
				file.writeInt(subDeleteResult.getOnePageNumber());
			} else {
				file.seek(pageStartPointer + 4);
				file.writeInt(subDeleteResult.getOnePageNumber());
			}
		}

		if(subDeleteResult.isWholePageDeleted()) {
			if(key != -1) {
				deleteCell(file, key, pageHeader);
			} else {
				pageHeader = new PageHeader(file, pageNumber);
				if (pageHeader.getNumCells() == 0 && pageNumber != 0) {
					deleteResult.setWholePageDeleted(true);
					file.seek(pageStartPointer);
					file.writeByte(0x00);
				} else if(pageHeader.getNumCells() == 0) {
					buildBlankPage(file, 0);
					deleteResult.setWholePageDeleted(true);
					return deleteResult;
				} else if(pageHeader.getNumCells() == 1 && pageNumber != 0) {
					file.seek(pageStartPointer + cellLocations.get(0));
					deleteResult.setOnePageNumber(file.readInt());
					file.seek(pageStartPointer);
					file.writeByte(0x00);
				} else if(pageHeader.getNumCells() == 1) {
					file.seek(pageStartPointer + cellLocations.get(0));
					int onePageNumber = file.readInt();
					file.seek(onePageNumber * UtilityTools.pageSize);
					byte[] bytes = new byte[(int) UtilityTools.pageSize];
					file.seek(onePageNumber * UtilityTools.pageSize);
					file.writeByte(0x00);
					file.seek(pageStartPointer);
					file.write(bytes);
				} else {
					file.seek(pageStartPointer + cellLocations.get(cellLocations.size() - 1));
					int newRightPointer = file.readInt();
					file.seek(pageStartPointer + 4);
					file.writeInt(newRightPointer);
					pageHeader = new PageHeader(file, pageNumber);
					deleteCell(file, cellLocations.size() - 1, pageHeader);
				}
			}
		} else if(key != -1) {
			//Update the cell last record key.
			if (subDeleteResult.keyIsDeleted(dcpKey)) {
				DataCellPage dcp = new DataCellPage(file, pageStartPointer + cellLocations.get(key), false);
				int getHighestRightValue = getHighestRightKeyValue(file, dcp.getLeftChildPointer());
				file.seek(pageStartPointer + cellLocations.get(key));
				file.writeInt(getHighestRightValue);
			}
		} else {
			if(pageHeader.getNumCells() == 0 && pageNumber != 0) {
				file.seek(pageStartPointer + 4);
				deleteResult.setOnePageNumber(file.readInt());
				file.seek(pageStartPointer);
				file.writeByte(0x00);
			} else if(pageHeader.getNumCells() == 0) {
				file.seek(pageStartPointer + 4);
				int onePageNumber = file.readInt();
				file.seek(onePageNumber * UtilityTools.pageSize);
				byte[] bytes = new byte[(int) UtilityTools.pageSize];
				file.seek(onePageNumber * UtilityTools.pageSize);
				file.writeByte(0x00);
				file.seek(pageStartPointer);
				file.write(bytes);
			}
		}
		deleteResult.mergeSubResult(subDeleteResult);
		return deleteResult;
	}

	private static void buildBlankPage(RandomAccessFile file, int pageNumber) throws IOException {
		long pageStart = pageNumber * UtilityTools.pageSize;
		file.seek(pageStart);
		file.writeByte(0x0D);
		file.writeByte(0x00);
		file.writeShort((int) UtilityTools.pageSize);
		file.writeInt(-1);
	}

	private static int getHighestRightKeyValue(RandomAccessFile file, int pageNumber) throws IOException {
		PageHeader pageHeader = new PageHeader(file, pageNumber);
		if(pageHeader.getPageType() > 0) {
			logMessage("WARNING SOMETHING WENT WRONG BADLY");
			return 0;
		}
		if(pageHeader.getPageType() == 0x05) {
			return getHighestRightKeyValue(file, pageHeader.getRightChiSibPointer());
		} else if(pageHeader.getPageType() == 0x0D) {
			DataCellPage dcp = new DataCellPage(file, pageHeader.getCellLocations().get(pageHeader.getNumCells() - 1), true);
			return dcp.getKey();
		} else {
			logMessage("Invalid page type. Get highest right value");
			return 0;
		}
	}

	private static void deleteCell(RandomAccessFile file, int key, PageHeader pageHeader) throws IOException {
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartFP = pageHeader.getPageStartFP();
		short searchKeyCellLocation = cellLocations.get(key);
		logMessage("DELETING cell in leaf");
		file.seek(pageStartFP + searchKeyCellLocation);
		short numOfBytes = 8;
		file.seek(pageStartFP + pageHeader.getCellContentStartOffset());
		int copyBytesLength = pageHeader.getCellContentStartOffset() - searchKeyCellLocation;
		byte[] copyBytes = new byte[copyBytesLength];
		file.read(copyBytes);
		file.seek(pageStartFP + pageHeader.getCellContentStartOffset() + numOfBytes);
		file.write(copyBytes);
		//Update cell content start
		file.seek(pageStartFP + 2);
		file.writeShort(pageHeader.getCellContentStartOffset() + numOfBytes);
		//Update cell locations
		int index = 0;
		for (Short cellLocation : cellLocations) {
			if(cellLocation < searchKeyCellLocation) {
				file.seek(pageStartFP + 8 + 2 * index);
				file.writeShort(cellLocation + numOfBytes);
			}
			index++;
		}
		//Remove the key cell locations in the header
		file.seek(pageStartFP + 8 + 2 * (key + 1));
		copyBytesLength = pageHeader.getHeaderEndOffset() - (8 + 2 * (key + 1));
		copyBytes = new byte[copyBytesLength];
		file.read(copyBytes);
		file.seek(pageStartFP + 8 + 2 * key);
		file.write(copyBytes);
		//Update the number of cells
		file.seek(pageStartFP + 1);
		file.writeByte(pageHeader.getNumCells() - 1);
	}

	private static void updateRightMostChildRightPointer(RandomAccessFile file, int pageNumber, int rightSiblingPageNumber) throws IOException {
		PageHeader pageHeader = new PageHeader(file, pageNumber);
		if(pageHeader.getPageType() == 0x05) {
			updateRightMostChildRightPointer(file, pageHeader.getRightChiSibPointer(), rightSiblingPageNumber);
		} else if(pageHeader.getPageType() == 0x0D) {
			file.seek(pageHeader.getPageStartFP() + 4);
			file.writeInt(rightSiblingPageNumber);
		} else {
			logMessage("Something is wrong invalid page traversed while updating right most child pointer!");
		}
	}

	private static int deleteRecord(RandomAccessFile file, int searchKey, PageHeader pageHeader) throws IOException {
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartFP = pageHeader.getPageStartFP();
		short searchKeyCellLocation = cellLocations.get(searchKey);
		file.seek(pageStartFP + searchKeyCellLocation);
		short numOfBytes = (short) (file.readShort() + 6);
		int pk = file.readInt();
		//Push the content to last.
		file.seek(pageStartFP + pageHeader.getCellContentStartOffset());
		int copyBytesLength = searchKeyCellLocation - pageHeader.getCellContentStartOffset();
		byte[] copyBytes = new byte[copyBytesLength];
		file.read(copyBytes);
		file.seek(pageStartFP + pageHeader.getCellContentStartOffset() + numOfBytes);
		file.write(copyBytes);
		//Update cell locations
		int index = 0;
		for (Short cellLocation : cellLocations) {
			if(cellLocation < searchKeyCellLocation) {
				file.seek(pageStartFP + 8 + 2 * index);
				file.writeShort(cellLocation + numOfBytes);
			}
			index++;
		}
		//Remove the key cell locations in the header
		file.seek(pageStartFP + 8 + 2 * (searchKey + 1));
		copyBytesLength = pageHeader.getHeaderEndOffset() - (8 + 2 * (searchKey + 1));
		copyBytes = new byte[copyBytesLength];
		file.read(copyBytes);
		file.seek(pageStartFP + 8 + 2 * searchKey);
		file.write(copyBytes);
		//Update the number of cells
		file.seek(pageStartFP + 1);
		file.writeByte(pageHeader.getNumCells() - 1);
		//Update cell offset
		file.writeShort(pageHeader.getCellContentStartOffset() + numOfBytes);

		file.seek(pageStartFP + searchKeyCellLocation + 2);
		return pk;
	}

	private static void displayHeader(RandomAccessFile file, PageHeader pageHeader) throws IOException {
		file.seek(pageHeader.getPageStartFP());
		StringBuilder s = new StringBuilder();
		s.append(file.readByte());
		int num = file.readByte();
		s.append(" ").append(num);
		s.append(" ").append(file.readShort());
		s.append(" ").append(file.readInt());
		for (int i = 0; i < num; i++)
			s.append(" ").append(file.readShort());
		logMessage(s.toString());
	}

	private static int traverseAndSelect(RandomAccessFile file, int pageNumber, List<Column> selectColumns, Table table, Condition condition, boolean checkCondition) throws IOException {
		PageHeader pageHeader = new PageHeader(file, pageNumber);
		if(condition != null && condition.column.isPk() && condition.operation.equals("is null"))
			return 0;
		if(condition != null && condition.column.isPk() && condition.operation.equals("is not null"))
			condition = null;
		if(pageHeader.getPageType() == 0x05) {
			//Current node is table interior
			if(condition != null && condition.column.isPk() && (condition.operation.equals(">") || condition.operation.equals(">=") || condition.operation.equals("="))) {
				//If operation is > or >= or = get pointer page to traverse.
				int leftPointerPage = getLeftPointerPage(file, pageHeader, condition);
				return traverseAndSelect(file, leftPointerPage, selectColumns, table, condition, true);
			} else {
				if(pageHeader.getNumCells() == 0)
					return 0;
				//Traverse to the left most cell location.
				DataCellPage dataCellPage = new DataCellPage(file, pageHeader.getPageStartFP() + pageHeader.getCellLocations().get(0), false);
				return traverseAndSelect(file, dataCellPage.getLeftChildPointer(), selectColumns, table, condition, true);
			}
		} else if (pageHeader.getPageType() == 0x0D) {
			//Current node is table leaf.
			List<Short> cellLocations = pageHeader.getCellLocations();
			long pageStartPointer = pageHeader.getPageStartFP();
			if(pageHeader.getNumCells() == 0)
				return 0;
			if(condition != null && condition.column.isPk() && (condition.operation.equals(">") || condition.operation.equals(">=") || condition.operation.equals("=") || condition.operation.equals("<") || condition.operation.equals("<="))) {
				if(condition.operation.equals("=")) {
					int searchKey = binarySearchKey(file, pageHeader, condition);
					if (searchKey == -1)
						return 0;
					displayRecord(file, pageStartPointer + cellLocations.get(searchKey), table, selectColumns);
					return 1;
				} else if(condition.operation.equals("<") || condition.operation.equals("<=")) {
					int numSelectedRows = 0;
					int key = largestKeyLesserEqual(file, pageHeader, condition, true);
					if(key == -1) {
						return 0;
					}
					if(condition.operation.equals("<")) {
						DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(key), true);
						if(condition.value.equal(dataCellPage.getKey()))
							key--;
					}
					for (int i = 0; i <= key; i++) {
						displayRecord(file, pageStartPointer + cellLocations.get(i), table, selectColumns);
						numSelectedRows++;
					}
					if(key == cellLocations.size() - 1 && pageHeader.getRightChiSibPointer() != -1)
						numSelectedRows += traverseAndSelect(file, pageHeader.getRightChiSibPointer(), selectColumns, table, condition, false);
					return numSelectedRows;
				} else {
					int numSelectedRows = 0;
					if(!checkCondition) {
						for (Short cellLocation : cellLocations) {
							displayRecord(file, pageStartPointer + cellLocation, table, selectColumns);
							numSelectedRows++;
						}
					} else {
						int key = smallestKeyGreaterEqual(file, pageHeader, condition, true);
						if(key == -1) {
							logMessage("Something went wrong. skGE is -1. Should not be in this page.");
							return 0;
						}
						if(condition.operation.equals(">")) {
							DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(key), true);
							if(condition.value.equal(dataCellPage.getKey()))
								key++;
						}
						for (int i = key; i < cellLocations.size(); i++) {
							displayRecord(file, pageStartPointer + cellLocations.get(i), table, selectColumns);
							numSelectedRows++;
						}
					}
					if(pageHeader.getRightChiSibPointer() != -1)
						numSelectedRows += traverseAndSelect(file, pageHeader.getRightChiSibPointer(), selectColumns, table, condition, false);
					return numSelectedRows;
				}
			} else {
				int numSelectedRows = 0;
				int columnIndex = -1;
				if(condition != null && !condition.column.isPk()) {
					columnIndex = table.getPkColumn().getOrdinalPosition() < condition.column.getOrdinalPosition() ? condition.column.getOrdinalPosition() - 1 : condition.column.getOrdinalPosition();
				}
				boolean conditionColumnIsPk = condition != null && condition.column.isPk();
				int index = 0;
				for (Short cellLocation : cellLocations) {
					file.seek(pageStartPointer + cellLocation);
					if(condition != null) {
						if(conditionColumnIsPk) {
							file.skipBytes(2);
							int pk = file.readInt();
							if(!condition.result(new IntType(pk)))
								continue;
						} else {
							file.skipBytes(6);
							int numColumns = file.readByte();
							if (columnIndex >= numColumns) {
								logMessage("Something very wrong happened to the database. Number of columns is less.");
								return 0;
							}
							byte type;
							for (int i = 0; i < columnIndex - 1; i++) {
								type = file.readByte();
								file.skipBytes(UtilityTools.getNumberOfBytesFromTypebyte(type));
							}
							type = file.readByte();
							switch (condition.operation) {
								case "is null":
									if (!UtilityTools.valueNull(type) && type != 0x0C)
										continue;
									break;
								case "is not null":
									if (UtilityTools.valueNull(type) || type == 0x0C)
										continue;
									break;
								default:
									if (UtilityTools.valueNull(type)) {
										continue;
									} else {
										int bytesLength = UtilityTools.getNumberOfBytesFromTypebyte(type);
										byte[] bytes = new byte[bytesLength];
										file.read(bytes);
										DataType tableValue = getDataTypeFromByteType(type, bytes);
										if (!condition.result(tableValue))
											continue;
									}
									break;
							}
						}
					}
					displayRecord(file, pageStartPointer + cellLocation, table, selectColumns);
					numSelectedRows++;
				}
				if(pageHeader.getRightChiSibPointer() != -1)
					numSelectedRows += traverseAndSelect(file, pageHeader.getRightChiSibPointer(), selectColumns, table, condition, false);
				return numSelectedRows;
			}
		} else {
			logMessage("Incorrect Page type");
			return 0;
		}
	}

	private static DataType getDataTypeFromByteType(byte type, byte[] bytes) {
		ByteBuffer bb = ByteBuffer.allocate(bytes.length);
		bb.put(bytes);
		switch (type) {
			case 0x04:
				return new TinyInt(bb.get(0));
			case 0x05:
				return new SmallInt(bb.getShort(0));
			case 0x06:
				return new IntType(bb.getInt(0));
			case 0x07:
				return new BigInt(bb.getLong(0));
			case 0x08:
				return new Real(bb.getFloat(0));
			case 0x09:
				return new DoubleType(bb.getDouble(0));
			case 0x0A:
				return new DateTimeType(bb.getLong(0));
			case 0x0B:
				return new DateType(bb.getLong(0));
			default:
				return new TextType(new String(bytes, StandardCharsets.US_ASCII));
		}
	}

	private static int smallestKeyGreaterEqual(RandomAccessFile file, PageHeader pageHeader, Condition condition, boolean isLeaf) throws IOException {
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartPointer = pageHeader.getPageStartFP();
		int left = 0, right = pageHeader.getNumCells() - 1, mid;
		DataCellPage leftDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(left), isLeaf);
		DataCellPage rightDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(right), isLeaf);
		if(condition.value.lesserEquals(leftDataCellPage.getKey()))
			return left;
		if(condition.value.greater(rightDataCellPage.getKey()))
			return -1;
		else if(condition.value.equal(rightDataCellPage.getKey()))
			return right;
		while(left != right) {
			mid = (left + right) / 2;
			DataCellPage midDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(mid), isLeaf);
			if(condition.value.greater(midDataCellPage.getKey()))
				left = mid + 1;
			else if(condition.value.lesser(midDataCellPage.getKey()))
				right = mid;
			else
				return mid;
		}
		return left;
	}

	private static int largestKeyLesserEqual(RandomAccessFile file, PageHeader pageHeader, Condition condition, boolean isLeaf) throws IOException {
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartPointer = pageHeader.getPageStartFP();
		int left = 0, right = pageHeader.getNumCells() - 1, mid;
		DataCellPage leftDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(left), isLeaf);
		DataCellPage rightDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(right), isLeaf);
		if(condition.value.greaterEquals(rightDataCellPage.getKey()))
			return right;
		else if(condition.value.lesser(leftDataCellPage.getKey()))
			return -1;
		else if(condition.value.equal(leftDataCellPage.getKey()))
			return left;

		while(left != right) {
			mid = (left + right) / 2;
			DataCellPage midDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(mid), isLeaf);
			if(condition.value.greater(midDataCellPage.getKey()))
				left = mid;
			else if(condition.value.lesser(midDataCellPage.getKey()))
				right = mid - 1;
			else
				return mid;
		}
		return left;
	}

	private static void displayRecord(RandomAccessFile file, long location, Table table, List<Column> selectColumns) throws IOException {
		file.seek(location);
		List<Integer> positions = new ArrayList<>();
		if(selectColumns != null) {
			for (Column column : selectColumns) {
				positions.add(column.getOrdinalPosition());
			}
		}
		HashMap<String, String> values = new HashMap<>();
		short payloadLength = file.readShort();
		if(payloadLength < 1) {
			logMessage("Payload empty. Please check again.");
		}
		Column pkColumn = table.getPkColumn();
		int pk = file.readInt();
		int pkPosition = pkColumn.getOrdinalPosition();
		if(positions.contains(pkPosition))
			values.put(pkColumn.getName(), pk + "");
		byte numOfColumns = file.readByte();
		if(numOfColumns < 1)
			logMessage("Payload empty. Please check again.");
		for(Map.Entry<String, Column> entry : table.getColumns().entrySet()) {
			Column column = entry.getValue();
			if(!column.isPk()) {
				byte typeByte = file.readByte();
				int numBytes = UtilityTools.getNumberOfBytesFromTypebyte(typeByte);
				if(positions.contains(column.getOrdinalPosition())) {
					byte[] byteArray = new byte[numBytes];
					file.read(byteArray);
					if(UtilityTools.valueNull(typeByte))
						values.put(column.getName(), "NULL");
					else
						values.put(column.getName(), column.getRecordValue(byteArray));
				} else {
					file.skipBytes(numBytes);
				}
			}
		}

		System.out.println("");
		if(selectColumns != null) {
			for (Column column : selectColumns) {
				System.out.print("|\t");
				System.out.format(column.getFormat(), values.get(column.getName()));
				System.out.print("\t|");
			}
		}
	}

	private static int binarySearchKey(RandomAccessFile file, PageHeader pageHeader, Condition condition) throws IOException {
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartPointer = pageHeader.getPageStartFP();
		int left = 0, right = pageHeader.getNumCells() - 1, mid;
		DataCellPage rightDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(right), true);
		DataCellPage leftDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(left), true);
		if(condition.value.greater(rightDataCellPage.getKey()))
			return -1;
		else if(condition.value.equal(rightDataCellPage.getKey()))
			return right;
		else if(condition.value.equal(leftDataCellPage.getKey()))
			return left;
		else if(condition.value.lesser(leftDataCellPage.getKey()))
			return -1;
		while(left <= right) {
			mid = (left + right) / 2;
			DataCellPage midDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(mid), true);
			if(condition.value.greater(midDataCellPage.getKey()))
				left = mid + 1;
			else if(condition.value.lesser(midDataCellPage.getKey()))
				right = mid - 1;
			else
				return mid;
		}
		return -1;
	}

	private static int getLeftPointerPage(RandomAccessFile file, PageHeader pageHeader, Condition condition) throws IOException {
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartPointer = pageHeader.getPageStartFP();
		short left = 0, right = (short) (cellLocations.size() - 1), mid;
		DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(right), false);
		if(condition.value.greater(dataCellPage.getKey()))
			return pageHeader.getRightChiSibPointer();
		else if(condition.value.equal(dataCellPage.getKey())) {
			switch (condition.operation) {
				case ">=":
					return dataCellPage.getLeftChildPointer();
				case "=":
					return dataCellPage.getLeftChildPointer();
				case ">":
					return pageHeader.getRightChiSibPointer();
				default:
					logMessage("This function should not be called for this type.");
					return -1;
			}
		}

		while(left != right) {
			mid = (short) ((left + right) / 2);
			dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(mid), false);
			if(condition.value.greater(dataCellPage.getKey()))
				left = (short) (mid + 1);
			if(condition.value.lesser(dataCellPage.getKey()))
				right = mid;
			else {
				switch (condition.operation) {
					case ">=":
						return dataCellPage.getLeftChildPointer();
					case "=":
						return dataCellPage.getLeftChildPointer();
					case ">":
						dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(mid + 1), false);
						return dataCellPage.getLeftChildPointer();
					default:
						logMessage("This function should not be called for this type.");
						return -1;
				}
			}
		}
		return new DataCellPage(file, pageStartPointer + cellLocations.get(left), false).getLeftChildPointer();
	}

	private static SplitPage traverseAndInsert(RandomAccessFile tableFile, int primaryKey, int pageNumber, LinkedHashMap<String, String> values, Table table, byte[] record) throws IOException, ParseException {
		PageHeader pageHeader = new PageHeader(tableFile, pageNumber);
		if(pageHeader.getPageType() == 0x05) {
			return traverseAndInsertInterior(tableFile, pageNumber, pageHeader, primaryKey, values, table, record);
		} else if (pageHeader.getPageType() == 0x0D) {
			return traverseAndInsertLeaf(tableFile, pageNumber, pageHeader, primaryKey, values, table, record);
		} else {
			logMessage("Incorrect Page type");
			return new SplitPage(false, 0);
		}
	}

	private static SplitPage traverseAndInsertInterior(RandomAccessFile tableFile, int pageNumber, PageHeader pageHeader, int primaryKey, LinkedHashMap<String, String> values, Table table, byte[] record) throws IOException, ParseException {
		short left = 0, right = (short) (pageHeader.getNumCells() - 1), mid;
		List<Short> cellLocations = pageHeader.getCellLocations();
		DataCellPage dataCellPage = new DataCellPage(tableFile, pageHeader.getPageStartFP() + cellLocations.get(right));
		if(primaryKey > dataCellPage.getKey()) {
			SplitPage splitPage = traverseAndInsert(tableFile, primaryKey, pageHeader.getRightChiSibPointer(), values, table, null);
			if(splitPage.isShouldSplit()) {
				//Check if space is available otherwise split.
				return checkAndSplitInteriorPage(tableFile, pageHeader, pageNumber, splitPage, (short) -1, 0);
			} else
				return splitPage;
		} else if(primaryKey == dataCellPage.getKey()) {
			if(values != null) {
				logMessage("Primary key must be unique.");
				return new SplitPage(false, -1);
			} else {
				SplitPage splitPage = traverseAndInsert(tableFile, primaryKey, dataCellPage.getLeftChildPointer(), null, table, null);
				if(splitPage.isShouldSplit()) {
					//Check if space is available otherwise split.
					return checkAndSplitInteriorPage(tableFile, pageHeader, pageNumber, splitPage, (short) -1, 0);
				} else
					return splitPage;
			}
		}
		while(left != right) {
			mid = (short) ((left + right) / 2);
			dataCellPage = new DataCellPage(tableFile, pageHeader.getPageStartFP() + cellLocations.get(mid));
			if(primaryKey < dataCellPage.getKey())
				right = mid;
			else if(primaryKey > dataCellPage.getKey())
				left = (short) (mid + 1);
			else
				break;
		}

		if(left == right) {
			SplitPage splitPage = traverseAndInsert(tableFile, primaryKey, pageHeader.getRightChiSibPointer(), values, table, null);
			if(splitPage.isShouldSplit()) {
				//Check if space is available otherwise split.
				dataCellPage = new DataCellPage(tableFile, pageHeader.getPageStartFP() + cellLocations.get(left));
				return checkAndSplitInteriorPage(tableFile, pageHeader, pageNumber, splitPage, left, dataCellPage.getLeftChildPointer());
			} else {
				return splitPage;
			}
		} else {
			if(values != null) {
				logMessage("Primary key must be unique.");
				return new SplitPage(false, -1);
			} else {
				SplitPage splitPage = traverseAndInsert(tableFile, primaryKey, dataCellPage.getLeftChildPointer(), null, table, null);
				if(splitPage.isShouldSplit()) {
					//Check if space is available otherwise split.
					return checkAndSplitInteriorPage(tableFile, pageHeader, pageNumber, splitPage, (short) -1, 0);
				} else
					return splitPage;
			}
		}
	}

	private static SplitPage traverseAndInsertLeaf(RandomAccessFile tableFile, int pageNumber, PageHeader pageHeader, int primaryKey, LinkedHashMap<String, String> values, Table table, byte[] record) throws IOException, ParseException {
		short space = (short) (pageHeader.getCellContentStartOffset() - pageHeader.getHeaderEndOffset());
		short recordLength = values != null ? table.getRecordLength(values) : (short) record.length;
		List<Short> cellLocations = pageHeader.getCellLocations();
		if(recordLength + 2 < space) {
			short locationOffset = (short) (pageHeader.getCellContentStartOffset() - recordLength);
			if(pageHeader.getNumCells() == 0) {
				tableFile.seek(pageHeader.getPageStartFP() + pageHeader.getHeaderEndOffset());
				tableFile.writeShort(locationOffset);
			} else {
				short addPosition = addAtPosition(tableFile, pageHeader, primaryKey, true);
				if(addPosition == -1) {
					logMessage("LEAF: Primary Key must be unique");
					return new SplitPage(false, -1);
				} else {
					long positionFP = pageHeader.getPageStartFP() + 8 + 2 * addPosition;
					tableFile.seek(positionFP);
					byte[] copyBytesFromPosition = null;
					if(pageHeader.getNumCells() != addPosition) {
						copyBytesFromPosition= new byte[2 * (pageHeader.getNumCells() - addPosition)];
						tableFile.read(copyBytesFromPosition);
						tableFile.seek(positionFP);
					}
					tableFile.writeShort(locationOffset);
					if(pageHeader.getNumCells() != addPosition && copyBytesFromPosition != null)
						tableFile.write(copyBytesFromPosition);
				}
			}
			tableFile.seek(pageHeader.getPageStartFP() + 1);
			tableFile.writeByte(pageHeader.getNumCells() + 1);
			tableFile.writeShort(locationOffset);
			//Update cell content start.
			tableFile.seek(pageHeader.getPageStartFP() + 2);
			tableFile.writeShort(locationOffset);
			int recordSize;
			if(values != null) {
				recordSize = table.writeRecord(tableFile, values, pageHeader.getPageStartFP() + locationOffset);
			} else {
				tableFile.seek(pageHeader.getPageStartFP() + locationOffset);
				tableFile.write(record);
				recordSize = record.length;
			}
			/*
			List<Column> selectColumns = new ArrayList<>();
			for (Map.Entry<String, Column> entry : table.getColumns().entrySet()) {
				selectColumns.add(entry.getValue());
			}
			*/
			//displayRecord(tableFile, pageHeader.getPageStartFP() + locationOffset, table, selectColumns);
			//logMessage("Inserted at :" + (pageHeader.getPageStartFP() + locationOffset));
			return new SplitPage(true, recordSize);
		} else {
			//Need to split page to make space.
			if(pageHeader.getNumCells() == 0) {
				logMessage("Wants to split a empty page!!");
				return new SplitPage(false, -1);
			}
			short addPosition = addAtPosition(tableFile, pageHeader, primaryKey, true);
			if(addPosition == -1) {
				logMessage("Primary key must be unique");
				return new SplitPage(false, -1);
			}
			int mid = pageHeader.getNumCells() / 2;
			List<Record> leftPage = new ArrayList<>();
			List<Record> rightPage = new ArrayList<>();
			//get the left page records
			int cellLocIndex = 0;
			Record recordObject = values != null ? table.getRecord(values) : new Record(record);
			for (int i = 0; i <= mid; i++) {
				if (i == addPosition) {
					leftPage.add(recordObject);
				} else {
					leftPage.add(new Record(tableFile, pageHeader.getPageStartFP() + cellLocations.get(cellLocIndex), true));
					cellLocIndex++;
				}
				tableFile.seek(pageHeader.getPageStartFP() + cellLocations.get(cellLocIndex - 1));
			}
			//If position is mid then the key is the to be inserted key else it is the last key added to the left page.
			int midKey;
			if(addPosition == mid) {
				midKey = primaryKey;
			} else {
				DataCellPage midCell = new DataCellPage(tableFile, pageHeader.getPageStartFP() + cellLocations.get(cellLocIndex - 1), true);
				midKey = midCell.getKey();
			}
			//get the right page records
			for (int i = mid + 1; i < pageHeader.getNumCells() + 1; i++) {
				if (i == addPosition) {
					rightPage.add(recordObject);
				} else {
					rightPage.add(new Record(tableFile, pageHeader.getPageStartFP() + cellLocations.get(cellLocIndex), true));
					cellLocIndex++;
				}
			}
			long tableLength = tableFile.length();
			int rightPageNumber = (int) (tableLength / UtilityTools.pageSize);
			int leftPageNumber = (pageNumber == 0)? rightPageNumber + 1 : pageNumber;
			if(pageNumber != 0)
				tableFile.setLength(tableLength + UtilityTools.pageSize);
			else
				tableFile.setLength(tableLength + 2 * UtilityTools.pageSize);
			buildNewPageFromRecords(tableFile, leftPageNumber * UtilityTools.pageSize, leftPage, rightPageNumber, true);
			buildNewPageFromRecords(tableFile, rightPageNumber * UtilityTools.pageSize, rightPage, pageHeader.getRightChiSibPointer(), true);
			if(addPosition == mid)
				midKey = primaryKey;
			if(pageNumber == 0) {
				tableFile.seek(pageHeader.getPageStartFP());
				tableFile.writeByte(0x05);
				tableFile.writeByte(1);
				tableFile.writeShort((int) (UtilityTools.pageSize - 8));
				tableFile.writeInt(rightPageNumber);
				tableFile.writeShort((int) (UtilityTools.pageSize - 8));
				tableFile.seek(pageHeader.getPageStartFP() + (UtilityTools.pageSize - 8));
				tableFile.writeInt(leftPageNumber);
				tableFile.writeInt(midKey);
				return new SplitPage(true, recordObject.getRecordLength());
			} else {
				return new SplitPage(midKey, rightPageNumber, recordObject.getRecordLength());
			}
		}
	}


	private static void buildNewPageFromRecords(RandomAccessFile file, long pageStartFP, List<Record> pageRecords, int rightPointer, boolean isLeaf) throws IOException {
		file.seek(pageStartFP);
		file.writeByte(isLeaf ? 0x0D : 0x05);
		file.write(pageRecords.size());
		file.skipBytes(2);
		file.writeInt(rightPointer);
		byte[] bytes;
		short locationOffset = (short) UtilityTools.pageSize;
		int recordIndex = 0;
		for (Record record : pageRecords) {
			bytes = record.getBytes();
			locationOffset = (short) (locationOffset - bytes.length);
			file.seek(pageStartFP + 8 + 2 * recordIndex);
			file.writeShort(locationOffset);
			file.seek(pageStartFP + locationOffset);
			file.write(bytes);
			recordIndex++;
		}
		file.seek(pageStartFP + 2);
		file.writeShort(locationOffset);
	}

	private static SplitPage checkAndSplitInteriorPage(RandomAccessFile tableFile, PageHeader pageHeader, int pageNumber, SplitPage splitPage, short position, int oldLeftPointer) throws IOException {
		short space = (short) (pageHeader.getCellContentStartOffset() - pageHeader.getHeaderEndOffset());
		short recordLength = 8;
		if(recordLength + 2 < space) {
			//Space is available for the new key.
			short locationOffset = (short) (pageHeader.getCellContentStartOffset() - recordLength);
			tableFile.seek(pageHeader.getPageStartFP() + locationOffset);
			if(position == -1) {
				//Key should be added in the end.
				//Write the new key and its left pointer which should be the right pointer of the page
				tableFile.writeInt(pageHeader.getRightChiSibPointer());
				tableFile.writeInt(splitPage.getKey());
				//Update the new right pointer of the page to the new page created.
				tableFile.seek(pageHeader.getPageStartFP() + 4);
				tableFile.writeInt(splitPage.getPageNumber());
				//Add the new cellLocation in the header
				tableFile.seek(pageHeader.getPageStartFP() + pageHeader.getHeaderEndOffset());
				tableFile.writeShort(locationOffset);
			} else {
				//Key should be added in between.
				//Write the new key and its left pointer which should be the left pointer of the previous position key
				tableFile.writeInt(oldLeftPointer);
				tableFile.writeInt(splitPage.getKey());
				//Write the new page number to past key here.
				tableFile.seek(pageHeader.getPageStartFP() + 8 + 2 * position);
				short cellLocation = tableFile.readShort();
				tableFile.seek(pageHeader.getPageStartFP()+ cellLocation);
				tableFile.writeInt(splitPage.getPageNumber());
				//Copy bytes from position to end
				tableFile.seek(pageHeader.getPageStartFP() + 8 + 2 * position);
				short bytesLength = (short) (pageHeader.getHeaderEndOffset() - 8 - 2 * position);
				byte[] bytes = new byte[bytesLength];
				tableFile.read(bytes, 0, bytesLength);
				//Write new added key.
				tableFile.seek(pageHeader.getPageStartFP() + 8 + 2 * position);
				tableFile.writeShort(locationOffset);
				//Write the copied bytes.
				tableFile.write(bytes);
			}
			//Increment no of cells
			tableFile.seek(pageHeader.getPageStartFP() + 1);
			tableFile.writeByte(pageHeader.getNumCells() + 1);
			//Update the Content start area of header
			tableFile.writeShort(locationOffset);
			//Return saying that the row is inserted.
			return new SplitPage(true, splitPage.getInsertedSize());
		} else {
			//Need to split to make space.
			List<Short> cellLocations = pageHeader.getCellLocations();
			//If position is not last, update the present position's left child pointer to the new page pointer.
			if(position != -1) {
				tableFile.seek(pageHeader.getPageStartFP() + 8 + 2 * position);
				short cellLocation = tableFile.readShort();
				tableFile.seek(pageHeader.getPageStartFP()+ cellLocation);
				tableFile.writeInt(splitPage.getPageNumber());
			} else {
				oldLeftPointer = pageHeader.getRightChiSibPointer();
			}

			//Get the mid position of the locations.
			int mid = pageHeader.getNumCells() / 2;
			List<Record> leftPage = new ArrayList<>();
			List<Record> rightPage = new ArrayList<>();
			InteriorCell newInteriorCell = new InteriorCell(oldLeftPointer, splitPage.getKey());
			//get the left page records
			int cellLocIndex = 0;
			for (int i = 0; i < mid; i++) {
				if (i == position) {
					leftPage.add(newInteriorCell.getRecord());
				} else {
					leftPage.add(new Record(tableFile, pageHeader.getPageStartFP() + cellLocations.get(cellLocIndex), false));
					cellLocIndex++;
				}
			}
			//If position is mid then the key is the to be inserted key else it is the last key added to the left page.
			int midKey;
			int midLeftPointer;
			if(position == mid) {
				midKey = newInteriorCell.getKey();
				midLeftPointer = newInteriorCell.getLeftChildPointer();
			} else {
				DataCellPage midCell = new DataCellPage(tableFile, pageHeader.getPageStartFP() + cellLocations.get(cellLocIndex));
				midKey = midCell.getKey();
				midLeftPointer = midCell.getLeftChildPointer();
				cellLocIndex++;
			}

			//get the right page records
			for (int i = mid + 1; i < pageHeader.getNumCells() + 1; i++) {
				if (i == position) {
					rightPage.add(newInteriorCell.getRecord());
				} else if(cellLocIndex < pageHeader.getNumCells()) {
					rightPage.add(new Record(tableFile, pageHeader.getPageStartFP() + cellLocations.get(cellLocIndex), false));
					cellLocIndex++;
				}
			}
			//If position is last, add to right page.
			if(position == -1)
				rightPage.add(newInteriorCell.getRecord());

			long tableLength = tableFile.length();
			//Create new page for right page number.
			int rightPageNumber = (int) (tableLength / UtilityTools.pageSize);
			//If root page create new page for left page number.
			int leftPageNumber = (pageNumber == 0)? rightPageNumber + 1 : pageNumber;
			//Set length to extend according to the number of pages to be created.
			if(pageNumber != 0)
				tableFile.setLength(tableLength + UtilityTools.pageSize);
			else
				tableFile.setLength(tableLength + 2 * UtilityTools.pageSize);

			//Start building the pages.
			//Left page's right pointer must be the mid's left pointer.
			buildNewPageFromRecords(tableFile, leftPageNumber * UtilityTools.pageSize, leftPage, midLeftPointer, false);
			//Right page's right pointer will be the new child pointer if the key is added at last, otherwise it will be the old page's right pointer.
			buildNewPageFromRecords(tableFile, rightPageNumber * UtilityTools.pageSize, rightPage, position == -1 ? splitPage.getPageNumber() : pageHeader.getRightChiSibPointer(), false);

			//Check if page is root.
			if(pageNumber == 0) {
				//The page is root. So update it
				tableFile.seek(pageHeader.getPageStartFP());
				tableFile.writeByte(0x05);
				//Only one key i.e. mid key.
				tableFile.writeByte(1);
				tableFile.writeShort((int) (UtilityTools.pageSize - 8));
				//Right child pointer is the new right page number.
				tableFile.writeInt(rightPageNumber);
				//Write the cell location.
				tableFile.writeShort((int) (UtilityTools.pageSize - 8));
				//write the data cell
				tableFile.seek(pageHeader.getPageStartFP() + (UtilityTools.pageSize - 8));
				tableFile.writeInt(leftPageNumber);
				tableFile.writeInt(midKey);
				return new SplitPage(true, splitPage.getInsertedSize());
			} else {
				return new SplitPage(midKey, rightPageNumber, splitPage.getInsertedSize());
			}
		}
	}

	private static short addAtPosition(RandomAccessFile file, PageHeader pageHeader, int primaryKey, boolean isLeaf) throws IOException {
		short left = 0, right = (short) (pageHeader.getNumCells() - 1), mid;
		long pageStartPointer = pageHeader.getPageStartFP();
		List<Short> cellLocations = pageHeader.getCellLocations();
		DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(right), isLeaf);
		if(primaryKey > dataCellPage.getKey())
			return (short) (right + 1);
		else if (primaryKey == dataCellPage.getKey())
			return -1;

		while(left != right) {
			mid = (short) ((left + right) / 2);
			dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(mid), isLeaf);
			if(primaryKey > dataCellPage.getKey())
				left = (short) (mid + 1);
			else if (primaryKey < dataCellPage.getKey())
				right = mid;
			else
				return -1;
		}
		return left;
	}

	private static List<Column> traverseAndGetColumns(RandomAccessFile file, int pageNumber, Table table, String tableName, String dbName) throws IOException {
		PageHeader pageHeader = new PageHeader(file, pageNumber);
		if(pageHeader.getPageType() == 0x05) {
			//Traverse to the left most cell location.
			DataCellPage dataCellPage = new DataCellPage(file, pageHeader.getPageStartFP() + pageHeader.getCellLocations().get(0), false);
			return traverseAndGetColumns(file, dataCellPage.getLeftChildPointer(), table, tableName, dbName);
		} else if (pageHeader.getPageType() == 0x0D) {
			//Current node is table leaf.
			List<Short> cellLocations = pageHeader.getCellLocations();
			long pageStartPointer = pageHeader.getPageStartFP();
			if(pageHeader.getNumCells() == 0)
				return new ArrayList<>();
			List<Column> list = new ArrayList<>();
			Column tableNameColumn = table.getColumns().get("table_name");
			Column dbNameColumn = table.getColumns().get("database_name");
			int tableColumnIndex = table.getPkColumn().getOrdinalPosition() < tableNameColumn.getOrdinalPosition() ? tableNameColumn.getOrdinalPosition() - 1 : tableNameColumn.getOrdinalPosition();
			int dbNameColumnIndex = table.getPkColumn().getOrdinalPosition() < dbNameColumn.getOrdinalPosition() ? dbNameColumn.getOrdinalPosition() - 1 : dbNameColumn.getOrdinalPosition();
			int minIndex = Integer.min(tableColumnIndex, dbNameColumnIndex);
			int maxIndex = Integer.max(tableColumnIndex, dbNameColumnIndex);
			DataType tableValueTableName;
			DataType tableValueDbName;
			DataType minIndexDataType;
			DataType maxIndexDataType;
			for (Short cellLocation : cellLocations) {
				file.seek(pageStartPointer + cellLocation);
				file.skipBytes(6);
				int numColumns = file.readByte();
				if (tableColumnIndex >= numColumns || dbNameColumnIndex >= numColumns) {
					logMessage("Something very wrong happened to the database. Number of columns is less.");
					return list;
				}
				byte type;
				int i;
				for (i = 0; i < minIndex - 1; i++) {
					type = file.readByte();
					file.skipBytes(UtilityTools.getNumberOfBytesFromTypebyte(type));
				}
				type = file.readByte();
				int bytesLength = UtilityTools.getNumberOfBytesFromTypebyte(type);
				byte[] bytes = new byte[bytesLength];
				file.read(bytes);
				minIndexDataType = getDataTypeFromByteType(type, bytes);

				for (i = minIndex; i < maxIndex - 1; i++) {
					type = file.readByte();
					file.skipBytes(UtilityTools.getNumberOfBytesFromTypebyte(type));
				}

				type = file.readByte();
				bytesLength = UtilityTools.getNumberOfBytesFromTypebyte(type);
				bytes = new byte[bytesLength];
				file.read(bytes);
				maxIndexDataType = getDataTypeFromByteType(type, bytes);

				tableValueTableName = minIndex == tableColumnIndex ? minIndexDataType : maxIndexDataType;
				tableValueDbName = minIndex == dbNameColumnIndex ? minIndexDataType : maxIndexDataType;
				if (!tableValueTableName.equal(tableName) || !tableValueDbName.equal(dbName))
					continue;
				list.add(getColumn(file, pageStartPointer + cellLocation, table));
			}
			if(pageHeader.getRightChiSibPointer() != -1)
				list.addAll(traverseAndGetColumns(file, pageHeader.getRightChiSibPointer(), table, tableName, dbName));
			return list;
		} else {
			logMessage("Incorrect Page type");
			return new ArrayList<>();
		}
	}

	private static Column getColumn(RandomAccessFile file, long location, Table table) throws IOException {
		file.seek(location);
		HashMap<String, String> values = new HashMap<>();
		short payloadLength = file.readShort();
		if(payloadLength < 1)
			logMessage("Payload empty. Please check again.");
		Column pkColumn = table.getPkColumn();
		int pk = file.readInt();
		values.put(pkColumn.getName(), pk + "");
		byte numOfColumns = file.readByte();
		if(numOfColumns < 1)
			logMessage("Payload empty. Please check again.");
		for(Map.Entry<String, Column> entry : table.getColumns().entrySet()) {
			String key = entry.getKey();
			Column column = entry.getValue();
			if(!column.isPk()) {
				byte typeByte = file.readByte();
				int numBytes = UtilityTools.getNumberOfBytesFromTypebyte(typeByte);
				byte[] byteArray = new byte[numBytes];
				file.read(byteArray);
				if(UtilityTools.valueNull(typeByte))
					values.put(column.getName(), "NULL");
				else
					values.put(column.getName(), column.getRecordValue(byteArray));
			}
		}
		return new Column(values.get("column_name"), values.get("data_type"), values.get("is_nullable").equals("YES"), Integer.parseInt(values.get("ordinal_position")), values.get("table_name"), values.get("database_name"), values.get("is_pk").equals("YES"));
	}

	private static void splashScreen() {
		System.out.println(line("-", 80));
		System.out.println("Welcome to ScyllaBase");
		System.out.println("ScyllaBase Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-", 80));
	}

	private static String line(String s, int num) {
		StringBuilder a = new StringBuilder();
		for (int i = 0; i < num; i++) {
			a.append(s);
		}
		return a.toString();
	}

	private static void help() {
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

	private static String getVersion() {
		return UtilityTools.version;
	}

	private static String getCopyright() {
		return UtilityTools.copyright;
	}

	private static void displayVersion() {
		System.out.println("ScyllaBase Version " + getVersion());
		System.out.println(getCopyright());
	}

	public static void parseUserCommand(String userCommand) {
		// String[] commandTokens = userCommand.split(" ");
		ArrayList<String> commandTokens = new ArrayList<>(Arrays.asList(userCommand.split(" ")));

		switch (commandTokens.get(0)) {
			case "select":
				parseSelectString(userCommand);
				break;
			case "show":
				if (commandTokens.get(1).equals("databases")) {
					parseShowDatabasesQuery();
				} else if (commandTokens.get(1).equals("tables")) {
					showTables();
				} else if (commandTokens.get(1).equals("columns")) {
					showColumns();
				} else {
					logMessage("Wrong type of create called");
					return;
				}
				break;
			case "drop":
				if (commandTokens.get(1).equals("database")) {
					parseDropDatabaseString(userCommand);
				} else if (commandTokens.get(1).equals("table")) {
					parseDropString(userCommand);
				} else {
					logMessage("Wrong type of create called");
					return;
				}
				break;
			case "create":
				if (commandTokens.get(1).equals("database")) {
					parseCreateDatabaseString(userCommand);
				} else if (commandTokens.get(1).equals("table")) {
					parseCreateString(userCommand);
				} else {
					logMessage("Wrong type of create called");
					return;
				}
				break;
			case "delete":
				parseDeleteString(userCommand);
				break;
			case "insert":
				parseInsertString(userCommand);
				break;
			case "use":
				parseUseDatabaseString(userCommand);
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
				break;
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				break;
		}
	}

	private static void parseSelectString(String userCommand) {
		Pattern pattern = Pattern.compile("^select (.+) from (\\S+)(?: where (.+)$|$)");
		Matcher matcher = pattern.matcher(userCommand);
		if(matcher.find()) {
			if(matcher.groupCount() == 3)
				checkAndExecuteSelectString(matcher.group(1), matcher.group(2), matcher.group(3));
			else
				wrongSyntax();
		} else
			wrongSyntax();
	}

	private static void checkAndExecuteSelectString(String selectColumns, String dbTableName, String condition) {
		//String[] tableSplit = tableName.split("\\.", 1);
		dbTableName = dbTableName.trim();
		String[] dbTableNameSplit = dbTableName.split("\\.");
		String tableName;
		String dbName;
		if(dbTableNameSplit.length > 1) {
			dbName = dbTableNameSplit[0];
			tableName = dbTableNameSplit[1];
		} else {
			if(currentDb == null) {
				displayError("No database selected");
				return;
			}
			dbName = currentDb;
			tableName = dbTableName;
		}
		File dbFile = new File("Database/" + dbName);
		if(!dbFile.exists() || !dbFile.isDirectory()) {
			displayError("Database does not exist.");
			return;
		}
		String fileName = "Database/" + dbName + "/" + tableName + ".tbl";
		File f = new File(fileName);
		if (!f.exists()) {
			displayError("Table does not exist.");
			return;
		}
		try {
			RandomAccessFile sBColumnsTableFile = new RandomAccessFile("Database/catalog/sb_columns.tbl", "rw");
			List<Column> columns = traverseAndGetColumns(sBColumnsTableFile, 0, UtilityTools.sbColumnsTable, tableName, dbName);
			sBColumnsTableFile.close();
			columns.sort((column, t1) -> new Integer(column.getOrdinalPosition()).compareTo(t1.getOrdinalPosition()));
			LinkedHashMap<String, Column> columnHashMap = new LinkedHashMap<>();
			for (Column column: columns) {
				columnHashMap.put(column.getName(), column);
			}
			List<Column> selectColumnsList = new ArrayList<>();
			if(selectColumns.trim().equals("*")) {
				selectColumnsList.addAll(columns);
			} else {
				for (String columnName : selectColumns.split(",")) {
					if (columnHashMap.get(columnName) != null)
						selectColumnsList.add(columnHashMap.get(columnName));
					else {
						wrongSyntax();
						return;
					}
				}
			}
			Table table = new Table(dbName, tableName, columnHashMap);
			RandomAccessFile tableFile = new RandomAccessFile(table.getFilePath(), "rw");
			Condition conditionObj = null;
			if(condition != null) {
				Pattern pattern = Pattern.compile("^(\\S+) (= |!= |> |>= |< |<= |like |is null$|is not null$)(.*)");
				Matcher matcher = pattern.matcher(condition);
				if(matcher.find()) {
					if (matcher.groupCount() == 3) {
						Column column = columnHashMap.get(matcher.group(1));
						if(column == null) {
							displayError("Condition Column not found");
							return;
						}
						String operationString = matcher.group(2).trim();
						if(matcher.group(3) != null && !matcher.group(3).equals("")) {
							if(operationString.equals("is null") || operationString.equals("is not null")) {
								wrongSyntax();
								return;
							}
							DataType columnValue = column.getColumnValue(matcher.group(3));
							if(columnValue instanceof TextType || columnValue instanceof DateTimeType || columnValue instanceof DateType) {
								if(UtilityTools.regexSatisfy((String) columnValue.getValue(), "^\".*\"$"))
									columnValue = new TextType(((String) columnValue.getValue()).replaceAll("^\"|\"$", ""));
								else if(UtilityTools.regexSatisfy((String) columnValue.getValue(), "^'.*'$"))
									columnValue = new TextType(((String) columnValue.getValue()).replaceAll("^'|'$", ""));
								else {
									displayError("String, dates, datetime columns must be string and must be in '' or \"\"");
									return;
								}
							}
							conditionObj = new Condition(column, operationString, columnValue);
						} else {
							if(!operationString.equals("is null") && !operationString.equals("is not null")) {
								wrongSyntax();
								return;
							}
							conditionObj = new Condition(column, operationString, null);
						}
					} else {
						wrongSyntax();
						return;
					}
				} else {
					displayError("Where condition problem.");
					return;
				}
			}
			displayTableHeader(selectColumnsList);
			traverseAndSelect(tableFile, 0, selectColumnsList, table, conditionObj, true);
			System.out.println();
		} catch (IOException | ParseException e) {
			e.printStackTrace();
		}
	}

	private static void wrongSyntax() {
		System.out.println("Wrong syntax please check again.");
	}

	public static void dropTable(String dropTableString) {
		System.out.println("STUB: Calling parseQueryString(String s) to process queries");
		System.out.println("Parsing the string:\"" + dropTableString + "\"");
	}

	public static void parseCreateString(String createTableString) {
		logMessage("Calling create on the query\n" + createTableString);
		Pattern createTablePattern = Pattern.compile("^create table ([^(]*)\\((.*)\\)$");
		Matcher commandMatcher = createTablePattern.matcher(createTableString);
		if(commandMatcher.find()) {
			String dbTableName = commandMatcher.group(1).trim();
			String[] dbTableNameSplit = dbTableName.split("\\.");
			String tableName;
			String dbName;
			if(dbTableNameSplit.length > 1) {
				dbName = dbTableNameSplit[0];
				tableName = dbTableNameSplit[1];
			} else {
				if(currentDb == null) {
					displayError("No database selected");
					return;
				}
				dbName = currentDb;
				tableName = dbTableName;
			}
			if(dbName.equalsIgnoreCase("catalog")) {
				displayError("You cannot create table inside catalog.");
				return;
			}
			File dbFile = new File("Database/" + dbName);
			if(!dbFile.exists() || !dbFile.isDirectory()) {
				displayError("Database does not exist.");
				return;
			}
			String fileName = "Database/" + dbName + "/" + tableName + ".tbl";
			File f = new File(fileName);
			if (f.exists()) {
				displayError("Table already exists");
				return;
			}
			String columnStringPart = commandMatcher.group(2).trim();
			String[] columnsStrings = columnStringPart.split(",");
			List<Column> columns = new ArrayList<>();
			columns.add(new Column("row_id", "INT", false, 1, tableName, dbName, true));
			int position = 2;
			for (String columnString : columnsStrings) {
				columnString = columnString.trim();
				String[] columnTypes = columnString.split(" ", 3);
				if (columnTypes.length < 2) {
					displayError("Table already exists");
					return;
				}
				columns.add(new Column(columnTypes[0], columnTypes[1], !(columnTypes.length == 3 && columnTypes[2].equals("NOT NULL")), position, tableName, dbName, false));
				position++;
			}
			LinkedHashMap<String, Column> columnsHashMap = new LinkedHashMap<>();
			for (Column column : columns) {
				columnsHashMap.put(column.getName(), column);
			}
			Table table = new Table(dbName, tableName, columnsHashMap);
			createTable(table);
		}
	}

	private static void createTable(Table table) {
		if(!table.checkCreation()) {
			displayError("Type not supported");
			return;
		}
		int tablesLastPk = getLastPk(sbTablesTable);
		int columnsLastPk = getLastPk(UtilityTools.sbColumnsTable);
		LinkedHashMap<String, String> tableValues = new LinkedHashMap<>();
		tableValues.put("row_id", (tablesLastPk + 1) + "");
		tableValues.put("table_name", table.getTableName());
		tableValues.put("database_name", table.getDbName());
		tableValues.put("record_count", "0");
		tableValues.put("avg_length", "0");
		try {
			RandomAccessFile sBTablesTableFile = new RandomAccessFile("Database/catalog/sb_tables.tbl", "rw");
			logMessage("Add Table Row");
			traverseAndInsert(sBTablesTableFile, Integer.parseInt(tableValues.get("row_id")), 0, tableValues, sbTablesTable, null);
			sBTablesTableFile.close();
		} catch (ParseException | IOException e) {
			e.printStackTrace();
		}
		try {
			RandomAccessFile sBTablesTableFile = new RandomAccessFile("Database/catalog/sb_columns.tbl", "rw");
			List<LinkedHashMap<String, String>> columnsList = getColumnValues(table, columnsLastPk);
			for (LinkedHashMap<String, String> values: columnsList) {
				traverseAndInsert(sBTablesTableFile, Integer.parseInt(values.get("row_id")), 0, values, UtilityTools.sbColumnsTable, null);
			}
			sBTablesTableFile.close();
		} catch (ParseException | IOException e) {
			e.printStackTrace();
		}
		try {
			RandomAccessFile tableFile = new RandomAccessFile(table.getFilePath(), "rw");
			tableFile.setLength(UtilityTools.pageSize);
			tableFile.seek(0);
			tableFile.writeByte(0x0D);
			tableFile.skipBytes(1);
			tableFile.writeShort((int) UtilityTools.pageSize);
			tableFile.writeInt(-1);
			tableFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		logMessage("Table Created");
	}

	private static List<LinkedHashMap<String, String>> getColumnValues(Table table, int pk) {
		List<LinkedHashMap<String, String>> list = new ArrayList<>();
		pk++;
		for (Map.Entry<String, Column> entry : table.getColumns().entrySet()) {
			Column column = entry.getValue();
			list.add(UtilityTools.getColumnsTableRow(column, pk));
			pk++;
		}
		return list;
	}

	private static int getLastPk(Table table) {
		return getLastPk(table, true);
	}

	private static int getLastPk(Table table, boolean isCatalog) {
		try {
			RandomAccessFile tableFile;
			if(isCatalog)
				tableFile = new RandomAccessFile("Database/catalog/" + table.getTableName() + ".tbl", "rw");
			else
				tableFile = new RandomAccessFile(table.getFilePath(), "rw");
			int key = traverseAndGetLastPk(tableFile, 0);
			tableFile.close();
			return key;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}

	private static void parseInsertString(String insertString) {
		logMessage("Calling create on the query\n" + insertString);
		Pattern createTablePattern = Pattern.compile("insert into ([^( ]+)(?:\\((.*)\\))? values\\((.+)\\)");
		Matcher commandMatcher = createTablePattern.matcher(insertString);
		if(commandMatcher.find()) {
			if(commandMatcher.group(1) == null || commandMatcher.group(3) == null) {
				wrongSyntax();
				return;
			}
			String dbTableName = commandMatcher.group(1).trim();
			String[] dbTableNameSplit = dbTableName.split("\\.");
			String tableName;
			String dbName;
			if(dbTableNameSplit.length > 1) {
				dbName = dbTableNameSplit[0];
				tableName = dbTableNameSplit[1];
			} else {
				if(currentDb == null) {
					displayError("No database selected");
					return;
				}
				dbName = currentDb;
				tableName = dbTableName;
			}
			if(dbName.equalsIgnoreCase("catalog")) {
				displayError("You cannot insert records inside catalog.");
				return;
			}
			File dbFile = new File("Database/" + dbName);
			if(!dbFile.exists() || !dbFile.isDirectory()) {
				displayError("Database does not exist.");
				return;
			}
			String fileName = "Database/" + dbName + "/" + tableName + ".tbl";
			File f = new File(fileName);
			if (!f.exists()) {
				displayError("Table does not exist");
				return;
			}
			List<Column> columns;
			try {
				RandomAccessFile sBColumnsTableFile = new RandomAccessFile("Database/catalog/sb_columns.tbl", "rw");
				columns = traverseAndGetColumns(sBColumnsTableFile, 0, UtilityTools.sbColumnsTable, tableName, dbName);
				columns.sort((column, t1) -> new Integer(column.getOrdinalPosition()).compareTo(t1.getOrdinalPosition()));
				sBColumnsTableFile.close();
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
			LinkedHashMap<String, Column> columnsHashMap = new LinkedHashMap<>();
			for (Column column : columns) {
				columnsHashMap.put(column.getName(), column);
			}
			Table table = new Table(dbName, tableName, columnsHashMap);
			LinkedHashMap<String, String> values = new LinkedHashMap<>();
			if(commandMatcher.group(2) != null) {
				String columnString = commandMatcher.group(2);
				String valuesString = commandMatcher.group(3);
				String[] columnStringSplit = columnString.trim().split(",");
				String[] valuesStringSplit = valuesString.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
				if (columnStringSplit.length != valuesStringSplit.length) {
					displayError("Incorrect number of columns and values specified");
					return;
				}
				for (int i = 0; i < columnStringSplit.length; i++) {
					values.put(columnStringSplit[i].trim(), valuesStringSplit[i].trim().replaceAll("^\"|\"$", "").replaceAll("^'|'$", ""));
				}
				values.computeIfAbsent("row_id", k -> (getLastPk(table, false) + 1) + "");
			} else {
				String valuesString = commandMatcher.group(3);
				String[] valuesStringSplit = valuesString.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
				if (columns.size() != valuesStringSplit.length) {
					displayError("Incorrect number of values specified");
					return;
				}
				for (int i = 0; i < valuesStringSplit.length; i++) {
					values.put(columns.get(i).getName(), valuesStringSplit[i].trim().replaceAll("^\"|\"$", "").replaceAll("^'|'$", ""));
				}
			}
			if(table.validateValues(values)) {
				try {
					RandomAccessFile tableFile = new RandomAccessFile(table.getFilePath(), "rw");
					SplitPage splitPage = traverseAndInsert(tableFile, Integer.parseInt(values.get("row_id")), 0, values, table, null);
					tableFile.close();
					if(splitPage.isInserted()) {
						System.out.println("Inserted value");
					}
				} catch (IOException | ParseException e) {
					e.printStackTrace();
				}
			} else {
				displayError("Incorrect values");
			}
		}
	}

	private static void parseDeleteString(String userCommand) {
		Pattern pattern = Pattern.compile("^delete from (\\S+)(?: where (.+)$|$)");
		Matcher matcher = pattern.matcher(userCommand);
		if(matcher.find()) {
			if(matcher.groupCount() == 2)
				checkAndExecuteDeleteString(matcher.group(1), matcher.group(2));
			else
				wrongSyntax();
		} else
			wrongSyntax();
	}

	private static void checkAndExecuteDeleteString(String dbTableName, String condition) {
		dbTableName = dbTableName.trim();
		String[] dbTableNameSplit = dbTableName.split("\\.");
		String tableName;
		String dbName;
		if(dbTableNameSplit.length > 1) {
			dbName = dbTableNameSplit[0];
			tableName = dbTableNameSplit[1];
		} else {
			if(currentDb == null) {
				displayError("No database selected");
				return;
			}
			dbName = currentDb;
			tableName = dbTableName;
		}
		if(dbName.equalsIgnoreCase("catalog")) {
			displayError("You cannot delete records inside catalog.");
			return;
		}
		File dbFile = new File("Database/" + dbName);
		if(!dbFile.exists() || !dbFile.isDirectory()) {
			displayError("Database does not exist.");
			return;
		}
		String fileName = "Database/" + dbName + "/" + tableName + ".tbl";
		File f = new File(fileName);
		if (!f.exists()) {
			displayError("Table does not exist.");
			return;
		}

		try {
			RandomAccessFile sBColumnsTableFile = new RandomAccessFile("Database/catalog/sb_columns.tbl", "rw");
			List<Column> columns = traverseAndGetColumns(sBColumnsTableFile, 0, UtilityTools.sbColumnsTable, tableName, dbName);
			sBColumnsTableFile.close();
			columns.sort((column, t1) -> new Integer(column.getOrdinalPosition()).compareTo(t1.getOrdinalPosition()));
			LinkedHashMap<String, Column> columnHashMap = new LinkedHashMap<>();
			for (Column column: columns) {
				columnHashMap.put(column.getName(), column);
			}
			Table table = new Table(dbName, tableName, columnHashMap);
			RandomAccessFile tableFile = new RandomAccessFile(table.getFilePath(), "rw");
			Condition conditionObj = null;
			if(condition != null) {
				Pattern pattern = Pattern.compile("^(\\S+) (= |!= |> |>= |< |<= |like |is null$|is not null$)(.*)");
				Matcher matcher = pattern.matcher(condition);
				if(matcher.find()) {
					if (matcher.groupCount() == 3) {
						Column column = columnHashMap.get(matcher.group(1));
						if(column == null) {
							displayError("Condition Column not found");
							return;
						}
						String operationString = matcher.group(2).trim();
						if(matcher.group(3) != null && !matcher.group(3).equals("")) {
							if(operationString.equals("is null") || operationString.equals("is not null")) {
								wrongSyntax();
								return;
							}
							DataType columnValue = column.getColumnValue(matcher.group(3));
							if(columnValue instanceof TextType || columnValue instanceof DateTimeType || columnValue instanceof DateType) {
								if(UtilityTools.regexSatisfy((String) columnValue.getValue(), "^\".*\"$"))
									columnValue = new TextType(((String) columnValue.getValue()).replaceAll("^\"|\"$", ""));
								else if(UtilityTools.regexSatisfy((String) columnValue.getValue(), "^'.*'$"))
									columnValue = new TextType(((String) columnValue.getValue()).replaceAll("^'|'$", ""));
								else {
									displayError("String, dates, datetime columns must be string and must be in '' or \"\"");
									return;
								}
							}
							conditionObj = new Condition(column, operationString, columnValue);
						} else {
							if(!operationString.equals("is null") && !operationString.equals("is not null")) {
								wrongSyntax();
								return;
							}
							conditionObj = new Condition(column, operationString, null);
						}
					} else {
						wrongSyntax();
					}
				}
			}
			DeleteResult deleteResult = traverseAndDelete(tableFile, 0, table, conditionObj);
			response(deleteResult.getNumOfRecordsDeleted() + "");
		} catch (IOException | ParseException e) {
			e.printStackTrace();
		}
	}

	private static void parseDropString(String userCommand) {
		Pattern pattern = Pattern.compile("^drop table ([^(\\s]*)$");
		Matcher matcher = pattern.matcher(userCommand);
		if(matcher.find()) {
			if(matcher.groupCount() == 1) {
				executeDropCommand(matcher.group(1).trim());
			} else {
				wrongSyntax();
			}
		}

	}

	private static void executeDropCommand(String dbTableName) {
		logMessage("Executing drop command");
		dbTableName = dbTableName.trim();
		String[] dbTableNameSplit = dbTableName.split("\\.");
		String tableName;
		String dbName;
		if(dbTableNameSplit.length > 1) {
			dbName = dbTableNameSplit[0];
			tableName = dbTableNameSplit[1];
		} else {
			if(currentDb == null) {
				displayError("No database selected");
				return;
			}
			dbName = currentDb;
			tableName = dbTableName;
		}
		if(dbName.equalsIgnoreCase("catalog")) {
			displayError("You cannot delete records inside catalog.");
			return;
		}
		File dbFile = new File("Database/" + dbName);
		if(!dbFile.exists() || !dbFile.isDirectory()) {
			displayError("Database does not exist.");
			return;
		}

		File f = new File("Database/" + dbName + "/" + tableName + ".tbl");
		if (!f.exists()) {
			displayError("Table does not exist");
			return;
		}
		try {
			RandomAccessFile sBTablesTableFile = new RandomAccessFile("Database/catalog/sb_tables.tbl", "rw");
			traverseAndDeleteCatalog(sBTablesTableFile, 0, sbTablesTable, tableName, dbName);
			sBTablesTableFile.close();
			RandomAccessFile sBColumnsTableFile = new RandomAccessFile("Database/catalog/sb_columns.tbl", "rw");
			traverseAndDeleteCatalog(sBColumnsTableFile, 0, sbColumnsTable, tableName, dbName);
			sBColumnsTableFile.close();
			if(f.delete())
				response("Successfully dropped the table " + tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static DeleteResult traverseAndDeleteCatalog(RandomAccessFile file, int pageNumber, Table table, String tableName, String dbName) throws IOException {
		PageHeader pageHeader = new PageHeader(file, pageNumber);
		DeleteResult deleteResult = new DeleteResult();
		if(pageHeader.getNumCells() == 0)
			return deleteResult;
		if(pageHeader.getPageType() == 0x05) {
			//Current node is table interior
			return traverseAndDeleteCatalogInterior(file, pageNumber, pageHeader, table, tableName, dbName);
		} else if(pageHeader.getPageType() == 0x0D) {
			return traverseAndDeleteCatalogLeaf(file, pageNumber, pageHeader, table, tableName, dbName);
		} else {
			logMessage("Invalid Page");
			return deleteResult;
		}
	}

	private static DeleteResult traverseAndDeleteCatalogLeaf(RandomAccessFile file, int pageNumber, PageHeader pageHeader, Table table, String tableName, String dbName) throws IOException {
		DeleteResult deleteResult = new DeleteResult();
		deleteResult.setLeaf(true);
		Column tableNameColumn = table.getColumns().get("table_name");
		Column dbNameColumn = table.getColumns().get("database_name");
		int tableColumnIndex = table.getPkColumn().getOrdinalPosition() < tableNameColumn.getOrdinalPosition() ? tableNameColumn.getOrdinalPosition() - 1 : tableNameColumn.getOrdinalPosition();
		int dbNameColumnIndex = table.getPkColumn().getOrdinalPosition() < dbNameColumn.getOrdinalPosition() ? dbNameColumn.getOrdinalPosition() - 1 : dbNameColumn.getOrdinalPosition();
		int minIndex = Integer.min(tableColumnIndex, dbNameColumnIndex);
		int maxIndex = Integer.max(tableColumnIndex, dbNameColumnIndex);
		DataType tableValueTableName;
		DataType tableValueDbName;
		DataType minIndexDataType;
		DataType maxIndexDataType;

		long pageStartPointer = pageHeader.getPageStartFP();
		List<Short> cellLocations = pageHeader.getCellLocations();
		int sizeOfCellLocations = cellLocations.size();
		for (int j = 0; j < sizeOfCellLocations; j++) {
			file.seek(pageStartPointer + cellLocations.get(j - deleteResult.getNumOfRecordsDeleted()));
			file.skipBytes(6);
			int numColumns = file.readByte();
			if (tableColumnIndex >= numColumns || dbNameColumnIndex >= numColumns) {
				logMessage("Something very wrong happened to the database. Number of columns is less.");
				return deleteResult;
			}

			byte type;
			int i;
			for (i = 0; i < minIndex - 1; i++) {
				type = file.readByte();
				file.skipBytes(UtilityTools.getNumberOfBytesFromTypebyte(type));
			}

			type = file.readByte();
			int bytesLength = UtilityTools.getNumberOfBytesFromTypebyte(type);
			byte[] bytes = new byte[bytesLength];
			file.read(bytes);
			minIndexDataType = getDataTypeFromByteType(type, bytes);

			for (i = minIndex; i < maxIndex - 1; i++) {
				type = file.readByte();
				file.skipBytes(UtilityTools.getNumberOfBytesFromTypebyte(type));
			}

			type = file.readByte();
			bytesLength = UtilityTools.getNumberOfBytesFromTypebyte(type);
			bytes = new byte[bytesLength];
			file.read(bytes);
			maxIndexDataType = getDataTypeFromByteType(type, bytes);

			tableValueTableName = minIndex == tableColumnIndex ? minIndexDataType : maxIndexDataType;
			tableValueDbName = minIndex == dbNameColumnIndex ? minIndexDataType : maxIndexDataType;
			if (!tableValueTableName.equal(tableName) || !tableValueDbName.equal(dbName))
				continue;
			int deletedPk = deleteRecord(file, j - deleteResult.getNumOfRecordsDeleted(), pageHeader);
			deleteResult.deleteKey(deletedPk);
			pageHeader = new PageHeader(file, pageNumber);
			cellLocations = pageHeader.getCellLocations();
			if(pageHeader.getNumCells() == 0 && pageNumber != 0) {
				deleteResult.setWholePageDeleted(true);
				deleteResult.setRightSiblingPageNumber(pageHeader.getRightChiSibPointer());
				deleteResult.setUpdateRightMostChildRightPointer(true);
				file.seek(pageStartPointer);
				file.write(0x00);
			}
		}
		return deleteResult;
	}

	private static DeleteResult traverseAndDeleteCatalogInterior(RandomAccessFile file, int pageNumber, PageHeader pageHeader, Table table, String tableName, String dbName) throws IOException {
		DeleteResult deleteResult = new DeleteResult();
		deleteResult.setLeaf(false);
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartPointer = pageHeader.getPageStartFP();
		int numCellsDeleted = 0;
		int cellLocationsSize = cellLocations.size();
		if(cellLocationsSize == 0) {
			logMessage("Something went wrong, no cells in this page but want to delete.");
			return deleteResult;
		}
		for (int i = 0; i < cellLocationsSize; i++) {
			//Delete Record and update page header.
			DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(i - numCellsDeleted), false);
			DeleteResult subDeleteResult = traverseAndDeleteCatalog(file, dataCellPage.getLeftChildPointer(), table, tableName, dbName);
			deleteResult = updateDeleteAfterTraversal(file, i - numCellsDeleted, pageHeader, pageNumber, deleteResult, subDeleteResult, dataCellPage.getKey());
			if(subDeleteResult.isWholePageDeleted())
				numCellsDeleted++;
			pageHeader = new PageHeader(file, pageNumber);
			cellLocations = pageHeader.getCellLocations();
		}
		DeleteResult subDeleteResult = traverseAndDeleteCatalog(file, pageHeader.getRightChiSibPointer(), table, tableName, dbName);
		deleteResult = updateDeleteAfterTraversal(file, -1, pageHeader, pageNumber, deleteResult, subDeleteResult, -1);
		return deleteResult;
	}

	private static void response(String response) {
		System.out.println(response);
	}

	private static int traverseAndGetLastPk(RandomAccessFile file, int pageNumber) throws IOException {
		PageHeader pageHeader = new PageHeader(file, pageNumber);
		if(pageHeader.getPageType() == 0x05) {
			if(pageHeader.getNumCells() == 0) {
				logMessage("Something is wrong");
				return 0;
			}
			return traverseAndGetLastPk(file, pageHeader.getRightChiSibPointer());
		} else if(pageHeader.getPageType() == 0x0D) {
			if(pageHeader.getNumCells() == 0) {
				logMessage("Empty page");
				return 0;
			}
			DataCellPage dataCellPage = new DataCellPage(file, pageHeader.getPageStartFP() + pageHeader.getCellLocations().get(pageHeader.getNumCells() - 1), true);
			return dataCellPage.getKey();
		} else {
			logMessage("Something is wrong");
			return 0;
		}
	}

	private static void parseCreateDatabaseString(String userCommand) {
		logMessage("Calling create on the query\n" + userCommand);
		Pattern createTablePattern = Pattern.compile("^create database ([a-z][a-z0-9]*)$");
		Matcher commandMatcher = createTablePattern.matcher(userCommand);
		if(commandMatcher.find()) {
			String dbName = commandMatcher.group(1).trim();
			if(dbName.equalsIgnoreCase("catalog")) {
				displayError("You cannot create catalog database.");
				return;
			}
			File dbFile = new File("Database/" + dbName);
			if(dbFile.exists() && dbFile.isDirectory()) {
				displayError("Database already exists.");
				return;
			}
			if(dbFile.mkdir())
				response("Database " + dbName + "created");
			else
				displayError("Something went wrong making the file.");
		} else
			wrongSyntax();
	}

	private static void parseDropDatabaseString(String userCommand) {
		logMessage("Calling create on the query\n" + userCommand);
		Pattern createTablePattern = Pattern.compile("^drop database ([a-z][a-z0-9]*)$");
		Matcher commandMatcher = createTablePattern.matcher(userCommand);
		if(commandMatcher.find()) {
			String dbName = commandMatcher.group(1).trim();
			if(dbName.equalsIgnoreCase("catalog")) {
				displayError("You cannot drop catalog database.");
				return;
			}
			File dbFile = new File("Database/" + dbName);
			if(!dbFile.exists() || !dbFile.isDirectory()) {
				displayError("Database does not exist.");
				return;
			}
			if(dbFile.delete())
				response("Database " + dbName + "dropped");
			else
				displayError("Something went wrong deleting the file.");
		} else
			wrongSyntax();
	}

	private static void parseUseDatabaseString(String userCommand) {
		logMessage("Calling create on the query\n" + userCommand);
		Pattern createTablePattern = Pattern.compile("^use ([a-z][a-z0-9]*)$");
		Matcher commandMatcher = createTablePattern.matcher(userCommand);
		if(commandMatcher.find()) {
			String dbName = commandMatcher.group(1).trim();
			File dbFile = new File("Database/" + dbName);
			if(!dbFile.exists() || !dbFile.isDirectory()) {
				displayError("Database does not exist.");
				return;
			}
			currentDb = dbName;
			response(dbName + " selected");
		} else
			wrongSyntax();
	}

	private static void parseUpdateString(String userCommand) {
		final String regex = "where(?=([^\"\\\\]*(\\\\.|\"([^\"\\\\]*\\\\.)*[^\"\\\\]*\"))*[^\"]*$)";
		final String updateWithoutWhereRegex = "^update (\\S*) set (.+)$";
		String[] stringSplit = userCommand.split(regex);
		String whereString = null;
		String updateStringWithoutWhere = null;
		if(stringSplit.length == 2) {
			updateStringWithoutWhere = stringSplit[0];
			whereString = stringSplit[1];
		} else if (stringSplit.length == 1) {
			updateStringWithoutWhere = stringSplit[0];
		} else {
			displayError("There cannot be more than one where keywords in the command.");
			return;
		}
		Pattern updateWithoutWherePattern = Pattern.compile(updateWithoutWhereRegex);
		final Matcher matcher = updateWithoutWherePattern.matcher(updateStringWithoutWhere.trim());
		if(matcher.find()) {
			String dbTableName = matcher.group(1);
			String setString = matcher.group(2);
			checkAndExecuteUpdateString(dbTableName, setString, whereString);
		} else {
			wrongSyntax();
		}
	}

	private static void checkAndExecuteUpdateString(String dbTableName, String setString, String condition) {
		dbTableName = dbTableName.trim();
		String[] dbTableNameSplit = dbTableName.split("\\.");
		String tableName;
		String dbName;
		if(dbTableNameSplit.length > 1) {
			dbName = dbTableNameSplit[0];
			tableName = dbTableNameSplit[1];
		} else {
			if(currentDb == null) {
				displayError("No database selected");
				return;
			}
			dbName = currentDb;
			tableName = dbTableName;
		}
		if(dbName.equalsIgnoreCase("catalog")) {
			displayError("You cannot update records inside catalog.");
			return;
		}
		File dbFile = new File("Database/" + dbName);
		if(!dbFile.exists() || !dbFile.isDirectory()) {
			displayError("Database does not exist.");
			return;
		}
		String fileName = "Database/" + dbName + "/" + tableName + ".tbl";
		File f = new File(fileName);
		if (!f.exists()) {
			displayError("Table does not exist.");
			return;
		}
		try {
			RandomAccessFile sBColumnsTableFile = new RandomAccessFile("Database/catalog/sb_columns.tbl", "rw");
			List<Column> columns = traverseAndGetColumns(sBColumnsTableFile, 0, UtilityTools.sbColumnsTable, tableName, dbName);
			sBColumnsTableFile.close();
			columns.sort((column, t1) -> new Integer(column.getOrdinalPosition()).compareTo(t1.getOrdinalPosition()));
			LinkedHashMap<String, Column> columnHashMap = new LinkedHashMap<>();
			for (Column column: columns) {
				columnHashMap.put(column.getName(), column);
			}
			Table table = new Table(dbName, tableName, columnHashMap);
			RandomAccessFile tableFile = new RandomAccessFile(table.getFilePath(), "rw");
			LinkedHashMap<String, String> values = new LinkedHashMap<>();
			if(setString == null) {
				displayError("Update command needs to update atleast one column using the set keyword.");
				return;
			} else {
				final String commaRegex = ",(?=([^\"\\\\]*(\\\\.|\"([^\"\\\\]*\\\\.)*[^\"\\\\]*\"))*[^\"]*$)";
				String[] valuesString = setString.split(commaRegex);
				Pattern setPattern = Pattern.compile("^(\\S+) = (.*)$");
				for (String valueString : valuesString) {
					valueString = valueString.trim();
					Matcher matcher = setPattern.matcher(valueString);
					if(matcher.find()) {
						if (matcher.groupCount() != 2) {
							wrongSyntax();
							return;
						}
						Column column = columnHashMap.get(matcher.group(1));
						if(column == null) {
							displayError("Set Column not found");
							return;
						}
						DataType columnValue = column.getColumnValue(matcher.group(2));
						if(columnValue instanceof TextType || columnValue instanceof DateTimeType || columnValue instanceof DateType) {
							if(!UtilityTools.regexSatisfy((String) columnValue.getValue(), "^\".*\"$") && !UtilityTools.regexSatisfy((String) columnValue.getValue(), "^'.*'$")) {
								displayError("String, dates, datetime columns must be string and must be in '' or \"\"");
								return;
							}
						}
						values.put(matcher.group(1).trim(), matcher.group(2).trim());
					} else {
						wrongSyntax();
						return;
					}
				}
			}
			Condition conditionObj = null;
			if(condition != null) {
				Pattern pattern = Pattern.compile("^(\\S+) (= |!= |> |>= |< |<= |like |is null$|is not null$)(.*)");
				Matcher matcher = pattern.matcher(condition);
				if(matcher.find()) {
					if (matcher.groupCount() == 3) {
						Column column = columnHashMap.get(matcher.group(1));
						if(column == null) {
							displayError("Condition Column not found");
							return;
						}
						String operationString = matcher.group(2).trim();
						if(matcher.group(3) != null && !matcher.group(3).equals("")) {
							if(operationString.equals("is null") || operationString.equals("is not null")) {
								wrongSyntax();
								return;
							}
							DataType columnValue = column.getColumnValue(matcher.group(3));
							if(columnValue instanceof TextType || columnValue instanceof DateTimeType || columnValue instanceof DateType) {
								if(UtilityTools.regexSatisfy((String) columnValue.getValue(), "^\".*\"$"))
									columnValue = new TextType(((String) columnValue.getValue()).replaceAll("^\"|\"$", ""));
								else if(UtilityTools.regexSatisfy((String) columnValue.getValue(), "^'.*'$"))
									columnValue = new TextType(((String) columnValue.getValue()).replaceAll("^'|'$", ""));
								else {
									displayError("String, dates, datetime columns must be string and must be in '' or \"\"");
									return;
								}
							}
							conditionObj = new Condition(column, operationString, columnValue);
						} else {
							if(!operationString.equals("is null") && !operationString.equals("is not null")) {
								wrongSyntax();
								return;
							}
							conditionObj = new Condition(column, operationString, null);
						}
					} else {
						wrongSyntax();
					}
				}
			}
			boolean updated = atomicTraverseAndUpdate(tableFile, table, conditionObj, values);
			response(updated ? "Successfully updated" : "not updated successfully");
		} catch (IOException | ParseException e) {
			e.printStackTrace();
		}
	}

	private static void parseShowDatabasesQuery() {
		File file = new File("Database/");
		String[] directories = file.list((current, name) -> new File(current, name).isDirectory());
		System.out.println("-----------------------------------------------------------------------------------------------------------------------------------------");
		for (String directory : directories) {
			System.out.print("|\t");
			System.out.format("%-20s", directory);
			System.out.print("\t|");
			System.out.println();
		}
		System.out.println("-----------------------------------------------------------------------------------------------------------------------------------------");
		System.out.println();
	}

	private static void displayError(String message) {
		System.out.println(message);
	}

	private static void logMessage(String message) {
		System.out.println(message);
	}
}