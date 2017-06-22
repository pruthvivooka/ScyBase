package com.scyllabase;

import java.nio.ByteBuffer;

public class InteriorCell {

	private int leftChildPointer;
	private int key;

	public InteriorCell(int leftChildPointer, int key) {
		this.leftChildPointer = leftChildPointer;
		this.key = key;
	}

	public int getLeftChildPointer() {
		return leftChildPointer;
	}

	public int getKey() {
		return key;
	}

	public Record getRecord() {
		ByteBuffer byteBuffer = ByteBuffer.allocate(8);
		byteBuffer.putInt(this.leftChildPointer);
		byteBuffer.putInt(this.key);
		return new Record(byteBuffer.array());
	}
}
