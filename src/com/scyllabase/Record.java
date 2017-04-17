package com.scyllabase;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

public class Record {

	private byte[] bytes = null;
	public Record(RandomAccessFile file, long pointer, Table table) throws IOException {
		file.seek(pointer);
		file.read(bytes, 0, file.readShort() + 6);
	}

	public byte[] getBytes() {
		return bytes;
	}
}
