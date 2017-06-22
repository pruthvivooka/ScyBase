package com.scyllabase.Commands;

import com.scyllabase.Column;
import com.scyllabase.Condition;
import com.scyllabase.Table;

import java.io.RandomAccessFile;
import java.util.List;

/**
 * Created by scy11a on 6/21/17.
 */
public class SelectParams {

	private RandomAccessFile file;
	private int pageNumber;
	private List<Column> selectColumns;
	private Table table;
	private Condition condition;
	private boolean checkCondition;

	public SelectParams(RandomAccessFile file, int pageNumber, List<Column> selectColumns, Table table, Condition condition, boolean checkCondition) {
		this.file = file;
		this.pageNumber = pageNumber;
		this.selectColumns = selectColumns;
		this.table = table;
		this.condition = condition;
		this.checkCondition = checkCondition;
	}

	public RandomAccessFile getFile() {
		return file;
	}

	public int getPageNumber() {
		return pageNumber;
	}

	public List<Column> getSelectColumns() {
		return selectColumns;
	}

	public Table getTable() {
		return table;
	}

	public Condition getCondition() {
		return condition;
	}

	public boolean isCheckCondition() {
		return checkCondition;
	}
}
