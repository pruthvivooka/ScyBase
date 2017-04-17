package com.scyllabase;

public class ColumnValue<E> {

	private E value;

	public ColumnValue(E init_value) {
		this.value = init_value;
	}

	public E getValue() {
		return this.value;
	}

}