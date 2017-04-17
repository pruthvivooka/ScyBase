package com.scyllabase;

import java.util.HashMap;
import java.util.LinkedHashMap;

public class UtilityTools {

	public static long pageSize = 512;
	public static String version = "1.0";
	public static String copyright = "Â©2017 Pruthvi Vooka";
	private static LinkedHashMap<String, Column> sbTablesColumns = new LinkedHashMap<>();
	private static LinkedHashMap<String, Column> sbColumnsColumns = new LinkedHashMap<>();
	static {
		sbTablesColumns.put("row_id", new Column("row_id", "INT", false, 1, "sb_tables", true));
		sbTablesColumns.put("table_name", new Column("table_name", "TEXT", false, 2, "sb_tables", false));
		sbTablesColumns.put("database_name", new Column("database_name", "TEXT", false, 3, "sb_tables", false));
		sbTablesColumns.put("record_count", new Column("record_count", "INT", false, 4, "sb_tables", false));
		sbTablesColumns.put("avg_length", new Column("avg_length", "SMALLINT", false, 5, "sb_tables", false));
		sbColumnsColumns.put("row_id", new Column("row_id", "INT", false, 1, "sb_columns", true));
		sbColumnsColumns.put("table_name", new Column("table_name", "TEXT", false, 2, "sb_columns", false));
		sbColumnsColumns.put("column_name", new Column("column_name", "TEXT", false, 3, "sb_columns", false));
		sbColumnsColumns.put("data_type", new Column("data_type", "TEXT", false, 4, "sb_columns", false));
		sbColumnsColumns.put("ordinal_position", new Column("ordinal_position", "INT", false, 5, "sb_columns", false));
		sbColumnsColumns.put("is_nullable", new Column("is_nullable", "TEXT", false, 6, "sb_columns", false));
	}

	public static Table sbTablesTable = new Table("sb_tables", sbTablesColumns);
	public static Table sbColumnsTable=  new Table("sb_columns", sbColumnsColumns);

}
