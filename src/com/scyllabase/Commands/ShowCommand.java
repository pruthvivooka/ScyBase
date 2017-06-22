package com.scyllabase.Commands;

import com.scyllabase.Column;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.scyllabase.ScyllaBase.displayError;
import static com.scyllabase.ScyllaBase.logMessage;
import static com.scyllabase.UtilityTools.sbColumnsTable;
import static com.scyllabase.UtilityTools.sbTablesTable;

/**
 * Created by scy11a on 6/21/17.
 */
public class ShowCommand implements Command {

	private String secondToken = null;
	private String currentDb;

	public ShowCommand(String secondToken, String currentDb) {
		this.secondToken = secondToken;
		this.currentDb = currentDb;
	}

	@Override
	public boolean execute() {
		if(this.secondToken == null) {
			displayError("Command not initialized");
			return false;
		}
		return parseShowString();
	}

	private boolean parseShowString() {
		if (secondToken.equals("databases")) {
			showDatabases();
		} else if (secondToken.equals("tables")) {
			showTables();
		} else if (secondToken.equals("columns")) {
			showColumns();
		} else {
			CommandHelper.wrongSyntax();
			return false;
		}
		return true;
	}

	private static void showDatabases() {
		File file = new File("Database/");
		String[] directories = file.list((current, name) -> new File(current, name).isDirectory());
		System.out.println("-----------------------------------------------------------------------------------------------------------------------------------------");
		if(directories != null) {
			for (String directory : directories) {
				System.out.print("|\t");
				System.out.format("%-20s", directory);
				System.out.print("\t|");
				System.out.println();
			}
		} else {
			System.out.println("No databases!");
		}
		System.out.println("-----------------------------------------------------------------------------------------------------------------------------------------");
		System.out.println();
	}

	private static void showTables() {
		try {
			RandomAccessFile sBTablesTableFile = new RandomAccessFile("Database/catalog/sb_tables.tbl", "rw");
			List<Column> selectColumns = new ArrayList<>();
			for (Map.Entry<String, Column> entry: sbTablesTable.getColumns().entrySet()) {
				selectColumns.add(entry.getValue());
			}
			CommandHelper.displayTableHeader(selectColumns);
			SelectCommand selectCommand = new SelectCommand(new SelectParams(sBTablesTableFile, 0, selectColumns, sbTablesTable, null, true));
			selectCommand.executeTraverseAndSelect();
			System.out.println();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void showColumns() {
		try {
			RandomAccessFile sBTablesTableFile = new RandomAccessFile("Database/catalog/sb_columns.tbl", "rw");
			List<Column> selectColumns = new ArrayList<>();
			for (Map.Entry<String, Column> entry: sbColumnsTable.getColumns().entrySet()) {
				selectColumns.add(entry.getValue());
			}
			CommandHelper.displayTableHeader(selectColumns);
			SelectCommand selectCommand = new SelectCommand(new SelectParams(sBTablesTableFile, 0, selectColumns, sbColumnsTable, null, true));
			selectCommand.executeTraverseAndSelect();
			System.out.println();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


}
