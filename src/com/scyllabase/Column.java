package com.scyllabase;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;

class Column {

	private String name;
	private String type;
	private boolean isNullable = false;
	private int ordinalPosition;
	private String tableName;

	Column(String name, String type, boolean isNullable, int ordinalPosition, String tableName) {
		this.name = name;
		this.type = type;
		this.isNullable = isNullable;
		this.ordinalPosition = ordinalPosition;
		this.tableName = tableName;
	}

	public String getName() {
		return name;
	}

	public String getType() {
		return type;
	}

	public boolean isNullable() {
		return isNullable;
	}

	public int getOrdinalPosition() {
		return ordinalPosition;
	}

	public String getTableName() {
		return tableName;
	}

	boolean check(String value) {
		if(value == null && !isNullable && !this.type.equals("TEXT"))
			return false;
		switch (this.type) {
			case "INT":
				try {
					Integer.parseInt(value);
				} catch (NumberFormatException e) {
					return false;
				}
				break;
			case "TINYINT":
				try {
					Byte.parseByte(value);
				} catch (NumberFormatException e) {
					return false;
				}
				break;
			case "SMALLINT":
				try {
					Short.parseShort(value);
				} catch (NumberFormatException e) {
					return false;
				}
				break;
			case "BIGINT":
				try {
					Long.parseLong(value);
				} catch (NumberFormatException e) {
					return false;
				}
				break;
			case "REAL":
				try {
					Float.parseFloat(value);
				} catch (NumberFormatException e) {
					return false;
				}
				break;
			case "DOUBLE":
				try {
					Double.parseDouble(value);
				} catch (NumberFormatException e) {
					return false;
				}
				break;
			case "DATETIME":
				SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
				try {
					parser.parse(value);
				} catch (ParseException e) {
					return false;
				}
				break;
			case "DATE":
				parser = new SimpleDateFormat("yyyy-MM-dd");
				try {
					parser.parse(value);
				} catch (ParseException e) {
					return false;
				}
				break;
			case "TEXT":
				break;
			default:
				return false;
		}
		return true;
	}

	public short getTypeLength(String value) {
		switch (this.type) {
			case "INT":
				return 4;
			case "TINYINT":
				return 1;
			case "SMALLINT":
				return 2;
			case "BIGINT":
				return 8;
			case "REAL":
				return 4;
			case "DOUBLE":
				return 8;
			case "DATETIME":
				return 8;
			case "DATE":
				return 8;
			case "TEXT":
				byte[] bytes = value.getBytes();
				return (short) bytes.length;
			default:
				return -1;
		}
	}

	public void writeValue(RandomAccessFile file, String value) throws IOException, ParseException {
		switch (this.type) {
			case "INT":
				if(value == null) {
					file.writeByte(0x02);
					file.writeInt(0);
				} else {
					file.writeByte(0x06);
					file.writeInt(Integer.parseInt(value));
				}
				break;
			case "TINYINT":
				if(value == null) {
					file.writeByte(0x00);
					file.writeByte(0x00);
				} else {
					file.writeByte(0x06);
					file.writeByte(Byte.parseByte(value));
				}
				break;
			case "SMALLINT":
				if(value == null) {
					file.writeByte(0x01);
					file.writeShort(0);
				} else {
					file.writeByte(0x05);
					file.writeShort(Short.parseShort(value));
				}
				break;
			case "BIGINT":
				if(value == null) {
					file.writeByte(0x03);
					file.writeLong(0);
				} else {
					file.writeByte(0x07);
					file.writeLong(Long.parseLong(value));
				}
				break;
			case "REAL":
				if(value == null) {
					file.writeByte(0x02);
					file.writeFloat(0);
				} else {
					file.writeByte(0x08);
					file.writeFloat(Float.parseFloat(value));
				}
				break;
			case "DOUBLE":
				if(value == null) {
					file.writeByte(0x03);
					file.writeDouble(0);
				} else {
					file.writeByte(0x09);
					file.writeDouble(Double.parseDouble(value));
				}
				break;
			case "DATETIME":
				SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
				Date value_date = parser.parse(value);
				Calendar calendar = Calendar.getInstance();
				calendar.setTime(value_date);
				long epochSeconds = calendar.getTimeInMillis() / 1000;
				file.writeLong(epochSeconds);
				break;
			case "DATE":
				parser = new SimpleDateFormat("yyyy-MM-dd");
				value_date = parser.parse(value);
				calendar = Calendar.getInstance();
				calendar.setTime(value_date);
				epochSeconds = calendar.getTimeInMillis() / 1000;
				file.writeLong(epochSeconds);
				break;
			case "TEXT":
				break;
		}
	}
}