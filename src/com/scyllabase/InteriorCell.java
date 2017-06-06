package com.scyllabase;

import java.nio.ByteBuffer;

class InteriorCell {

	private int leftChildPointer;
	private int key;

	InteriorCell(int leftChildPointer, int key) {
		this.leftChildPointer = leftChildPointer;
		this.key = key;
	}

	int getLeftChildPointer() {
		return leftChildPointer;
	}

	int getKey() {
		return key;
	}

	Record getRecord() {
		ByteBuffer byteBuffer = ByteBuffer.allocate(8);
		byteBuffer.putInt(this.leftChildPointer);
		byteBuffer.putInt(this.key);
		return new Record(byteBuffer.array());
	}
}
