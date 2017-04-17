package com.scyllabase;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class Table {

	private String tableName;
	private LinkedHashMap<String, Column> columns;
	private int recordCount;
	private int avgLength;


	public Table(String tableName, LinkedHashMap<String, Column> columns) {
		this.tableName = tableName;
		this.columns = columns;
	}

	public String getTableName() {
		return tableName;
	}

	public LinkedHashMap<String, Column> getColumns() {
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

	public boolean validateValues(LinkedHashMap<String, String> values) {
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

	public short getRecordLength(LinkedHashMap<String, String> values) {
		short recordLength = 6 + 1;
		for(Map.Entry<String, Column> entry : columns.entrySet()) {
			String key = entry.getKey();
			Column column = entry.getValue();
			if(!column.isPk())
				recordLength += column.getTypeLength(values.get(key)) + 1;
		}
		return recordLength;
	}

	public void writeRecord(RandomAccessFile file, LinkedHashMap<String, String> values, long position) throws IOException, ParseException {
		file.seek(position);
		file.writeShort(this.getRecordLength(values) - 6);
		int pk = -1;
		file.skipBytes(4);
		for(Map.Entry<String, Column> entry : columns.entrySet()) {
			String key = entry.getKey();
			Column column = entry.getValue();
			if(!column.isPk())
				column.writeValue(file, values.get(key));
			else
				pk = Integer.parseInt(values.get(key));
		}
		file.seek(position + 2);
		file.writeInt(pk);
	}
}