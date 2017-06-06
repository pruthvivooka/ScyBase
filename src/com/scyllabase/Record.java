package com.scyllabase;

import java.io.IOException;
import java.io.RandomAccessFile;

class Record {
	private byte[] bytes = null;
	Record(RandomAccessFile file, long pointer, boolean isLeaf) throws IOException {
		file.seek(pointer);
		int bytesLength = 8;
		if(isLeaf) {
			bytesLength = file.readShort() + 6;
			file.seek(pointer);
		}
		bytes = new byte[bytesLength];
		file.read(bytes, 0, bytesLength);
	}

	byte[] getBytes() {
		return bytes;
	}

	int getRecordLength() {
		return bytes.length;
	}

	Record(byte[] bytes) {
		this.bytes = bytes;
	}
}
