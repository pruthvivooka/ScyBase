package com.scyllabase;

import java.io.IOException;
import java.io.RandomAccessFile;

public class DataCellInterior {

	private int leftChildPointer;
	private int key;

	public DataCellInterior(RandomAccessFile file, long fpLocation) throws IOException {
		file.seek(fpLocation);
		this.leftChildPointer = file.readInt();
		this.key = file.readInt();
	}

	public int getLeftChildPointer() {
		return leftChildPointer;
	}

	public int getKey() {
		return key;
	}
}
