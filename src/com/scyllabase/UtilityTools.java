package com.scyllabase;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UtilityTools {

	public static long pageSize = 512;
	static String version = "1.0";
	static String copyright = "Â©2017 Pruthvi Vooka";
	private static LinkedHashMap<String, Column> sbTablesColumns = new LinkedHashMap<>();
	private static LinkedHashMap<String, Column> sbColumnsColumns = new LinkedHashMap<>();
	static {
		sbTablesColumns.put("row_id", new Column("row_id", "INT", false, 1, "sb_tables", "catalog", true));
		sbTablesColumns.put("table_name", new Column("table_name", "TEXT", false, 2, "sb_tables", "catalog", false));
		sbTablesColumns.put("database_name", new Column("database_name", "TEXT", false, 3, "sb_tables", "catalog", false));
		sbTablesColumns.put("record_count", new Column("record_count", "INT", false, 4, "sb_tables", "catalog", false));
		sbTablesColumns.put("avg_length", new Column("avg_length", "SMALLINT", false, 5, "sb_tables", "catalog", false));
		sbColumnsColumns.put("row_id", new Column("row_id", "INT", false, 1, "sb_columns", "catalog", true));
		sbColumnsColumns.put("table_name", new Column("table_name", "TEXT", false, 2, "sb_columns", "catalog", false));
		sbColumnsColumns.put("database_name", new Column("database_name", "TEXT", false, 3, "sb_columns", "catalog", false));
		sbColumnsColumns.put("column_name", new Column("column_name", "TEXT", false, 4, "sb_columns", "catalog", false));
		sbColumnsColumns.put("data_type", new Column("data_type", "TEXT", false, 5, "sb_columns", "catalog", false));
		sbColumnsColumns.put("ordinal_position", new Column("ordinal_position", "INT", false, 6, "sb_columns", "catalog", false));
		sbColumnsColumns.put("is_nullable", new Column("is_nullable", "TEXT", false, 7, "sb_columns", "catalog", false));
		sbColumnsColumns.put("is_pk", new Column("is_pk", "TEXT", false, 8, "sb_columns", "catalog", false));
	}

	public static Table sbTablesTable = new Table("catalog", "sb_tables", sbTablesColumns);
	public static Table sbColumnsTable=  new Table("catalog", "sb_columns", sbColumnsColumns);

	public static List<LinkedHashMap<String, String>> getSbColumnsTableValues() {
		List<LinkedHashMap<String, String>> list = new ArrayList<>();
		int pk = 1;
		for(Map.Entry<String, Column> entry : sbTablesColumns.entrySet()) {
			Column column = entry.getValue();
			list.add(getColumnsTableRow(column, pk));
			pk++;
		}
		for(Map.Entry<String, Column> entry : sbColumnsColumns.entrySet()) {
			Column column = entry.getValue();
			list.add(getColumnsTableRow(column, pk));
			pk++;
		}
		/*
		//For testing page splits.
		for(Map.Entry<String, Column> entry : sbColumnsColumns.entrySet()) {
			Column column = entry.getValue();
			list.add(getColumnsTableRow(column, pk));
			pk++;
		}
		for(Map.Entry<String, Column> entry : sbColumnsColumns.entrySet()) {
			Column column = entry.getValue();
			list.add(getColumnsTableRow(column, pk));
			pk++;
		}
		//list.add(getColumnsTableRow(new Column("is_nullablefldjfksdajlkfjlsdkjflksdjlkfjsdljflksdjfkj", "TEXT", false, 6, "sb_columns", false), pk));
		*/
		return list;
	}

	public static LinkedHashMap<String, String> getColumnsTableRow(Column column, int pk) {
		LinkedHashMap<String, String> hashMap = new LinkedHashMap<>();
		hashMap.put("row_id", pk+"");
		hashMap.put("table_name", column.getTableName());
		hashMap.put("database_name", column.getDbName());
		hashMap.put("column_name", column.getName());
		hashMap.put("data_type", column.getType());
		hashMap.put("ordinal_position", column.getOrdinalPosition() + "");
		hashMap.put("is_nullable", column.isNullable() ? "YES" : "NO");
		hashMap.put("is_pk", column.isPk() ? "YES" : "NO");
		return hashMap;
	}

	public static String applyRegexSubstitution(String string, String regex, String sub) {
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(string);
		return matcher.replaceAll(sub);
	}

	public static boolean regexSatisfy(String string, String regex) {
		return string.matches(regex);
	}

	public static int getNumberOfBytesFromTypebyte(byte type) {
		switch (type) {
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
				if(type >= 0x0C) {
					return type - 0x0C;
				} else
					return 0;
		}
	}

	public static boolean valueNull(byte type) {
		return type == 0x00 || type == 0x01 || type == 0x02 || type == 0x03;
	}
}