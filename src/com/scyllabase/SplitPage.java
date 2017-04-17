package com.scyllabase;

public class SplitPage {

	public int key;
	public long pageNumber;
	public boolean inserted;
	public boolean shouldSplit = false;

	public SplitPage(int key, long pageNumber, boolean inserted) {
		this.key = key;
		this.pageNumber = pageNumber;
		this.inserted = inserted;
		this.shouldSplit = true;
	}

	public SplitPage(boolean inserted) {
		this.inserted = inserted;
		this.shouldSplit = false;
	}

}
