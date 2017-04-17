package com.scyllabase;

public class SplitPage {

	private int key;
	private int pageNumber;
	private boolean inserted;
	private boolean shouldSplit = false;

	public SplitPage(int key, int pageNumber) {
		this.key = key;
		this.pageNumber = pageNumber;
		this.inserted = true;
		this.shouldSplit = true;
	}

	public SplitPage(boolean inserted) {
		this.inserted = inserted;
		this.shouldSplit = false;
	}

	public int getKey() {
		return key;
	}

	public int getPageNumber() {
		return pageNumber;
	}

	public boolean isInserted() {
		return inserted;
	}

	public boolean isShouldSplit() {
		return shouldSplit;
	}
}
