package com.scyllabase;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

public class Table {

	private String tableName;
	private HashMap<String, Column> columns;
	private int recordCount;
	private int avgLength;


	public Table(String tableName, HashMap<String, Column> columns) {
		this.tableName = tableName;
		this.columns = columns;
	}

	public String getTableName() {
		return tableName;
	}

	public HashMap<String, Column> getColumns() {
		return columns;
	}

	public int getAvgLength() {
		return avgLength;
	}

	public void setAvgLength(int avgLength) {
		this.avgLength = avgLength;
	}

	public int getRecordCount() {
		return recordCount;
	}

	public void setRecordCount(int recordCount) {
		this.recordCount = recordCount;
	}

	public boolean validateValues(HashMap<String, String> values) {
		if(columns.entrySet().size() != values.entrySet().size())
			return false;
		for(Map.Entry<String, Column> entry : columns.entrySet()) {
			String key = entry.getKey();
			Column column = entry.getValue();
			if (!column.check(values.get(key)))
				return false;
		}
		return getRecordLength(values) <= UtilityTools.pageSize - 10;
	}

	public short getRecordLength(HashMap<String, String> values) {
		short record_length = 0;
		for(Map.Entry<String, Column> entry : columns.entrySet()) {
			String key = entry.getKey();
			Column column = entry.getValue();
			record_length += column.getTypeLength(values.get(key)) + 1;
		}
		return record_length;
	}

	public void writeRecord(RandomAccessFile file, HashMap<String, String> values, long position) throws IOException, ParseException {
		file.seek(position);
		for(Map.Entry<String, Column> entry : columns.entrySet()) {
			String key = entry.getKey();
			Column column = entry.getValue();
			column.writeValue(file, values.get(key));
		}
	}
}