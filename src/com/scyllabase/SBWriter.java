package com.scyllabase;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

public class SBWriter extends RandomAccessFile {

    public SBWriter(String file_path) throws FileNotFoundException {
        super(file_path, "r");
    }
}
