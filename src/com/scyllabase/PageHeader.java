package com.scyllabase;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

class PageHeader {

	private long pageStartFP;
	private byte pageType;
	private byte numCells;
	private short cellContentStartOffset;
	private int rightChiSibPointer;
	private List<Short> cellLocations = new ArrayList<>();
	private short headerEndOffset;

	PageHeader(RandomAccessFile file, int pageNumber) {
		this.pageStartFP = pageNumber * UtilityTools.pageSize;
		try {
			file.seek(this.pageStartFP);
			this.pageType = file.readByte();
			this.numCells = file.readByte();
			this.cellContentStartOffset = file.readShort();
			this.rightChiSibPointer = file.readInt();
			for (int i = 0; i < this.numCells; i++)
				this.cellLocations.add(file.readShort());
			this.headerEndOffset = (short) (file.getFilePointer() - this.pageStartFP);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	byte getPageType() {
		return this.pageType;
	}

	short getCellContentStartOffset() {
		return this.cellContentStartOffset;
	}

	int getRightChiSibPointer() {
		return this.rightChiSibPointer;
	}

	List<Short> getCellLocations() {
		return this.cellLocations;
	}

	byte getNumCells() {
		return numCells;
	}

	long getPageStartFP() {
		return this.pageStartFP;
	}

	short getHeaderEndOffset() {
		return headerEndOffset;
	}

}
