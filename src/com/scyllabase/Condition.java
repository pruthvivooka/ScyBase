package com.scyllabase;

class Condition {

	Column column;
	String operation;
	DataType value;

	DataType getValue() {
		return this.value;
	}

	boolean result(DataType tableValue) {
		switch (this.operation) {
			case "=":
				return tableValue.equal(this.value.getValue());
			case "<":
				return tableValue.lesser(this.value.getValue());
			case "<=":
				return tableValue.lesserEquals(this.value.getValue());
			case ">":
				return tableValue.greater(this.value.getValue());
			case ">=":
				return tableValue.greaterEquals(this.value.getValue());
			case "like":
				return tableValue.like(this.value.getValue());
			case "!=":
				return tableValue.notEqual(this.value.getValue());
			case "LIKE":
				return tableValue.like(this.value.getValue());
			default:
				return true;
		}
	}

	Condition(Column column, String operation, DataType value) {
		this.column = column;
		this.operation = operation;
		this.value = value;
	}
}