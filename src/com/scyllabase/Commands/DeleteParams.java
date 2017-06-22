package com.scyllabase.Commands;

import com.scyllabase.Condition;
import com.scyllabase.Table;

import java.io.RandomAccessFile;

/**
 * Created by scy11a on 6/21/17.
 */
public class DeleteParams {

	private RandomAccessFile file;
	private int pageNumber;
	private Table table;
	private Condition condition;

	public DeleteParams(RandomAccessFile file, int pageNumber, Table table, Condition condition) {
		this.file = file;
		this.pageNumber = pageNumber;
		this.table = table;
		this.condition = condition;
	}

	public RandomAccessFile getFile() {
		return file;
	}

	public int getPageNumber() {
		return pageNumber;
	}

	public Table getTable() {
		return table;
	}

	public Condition getCondition() {
		return condition;
	}
}
