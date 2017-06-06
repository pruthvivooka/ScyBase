package com.scyllabase;

import java.util.ArrayList;
import java.util.List;

class DeleteResult {

	private int numOfRecordsDeleted;
	private List<Integer> deletedKeysList;
	private boolean wholePageDeleted = false;
	private int rightSiblingPageNumber = -1;
	private boolean updateRightMostChildRightPointer = false;
	private int onePageNumber = -1;
	private boolean isLeaf = false;

	DeleteResult() {
		this.numOfRecordsDeleted = 0;
		this.deletedKeysList = new ArrayList<>();
	}

	void deleteKey(int key) {
		this.numOfRecordsDeleted++;
		this.deletedKeysList.add(key);
	}

	int getNumOfRecordsDeleted() {
		return this.numOfRecordsDeleted;
	}

	boolean keyIsDeleted(int key) {
		return this.deletedKeysList.contains(key);
	}

	boolean isWholePageDeleted() {
		return this.wholePageDeleted;
	}

	int getRightSiblingPageNumber() {
		return this.rightSiblingPageNumber;
	}


	void setWholePageDeleted(boolean wholePageDeleted) {
		this.wholePageDeleted = wholePageDeleted;
	}

	void setRightSiblingPageNumber(int rightSiblingPageNumber) {
		this.rightSiblingPageNumber = rightSiblingPageNumber;
	}

	boolean isUpdateRightMostChildRightPointer() {
		return updateRightMostChildRightPointer;
	}

	void setUpdateRightMostChildRightPointer(boolean updateRightMostChildRightPointer) {
		this.updateRightMostChildRightPointer = updateRightMostChildRightPointer;
	}

	public boolean isLeaf() {
		return isLeaf;
	}

	void setLeaf(boolean leaf) {
		isLeaf = leaf;
	}

	void mergeSubResult(DeleteResult subDeleteResult) {
		this.numOfRecordsDeleted += subDeleteResult.numOfRecordsDeleted;
		this.deletedKeysList.addAll(subDeleteResult.deletedKeysList);
	}

	public int getOnePageNumber() {
		return onePageNumber;
	}

	public void setOnePageNumber(int onePageNumber) {
		this.onePageNumber = onePageNumber;
	}
}
