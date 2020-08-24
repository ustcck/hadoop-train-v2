package com.ustcck.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class HDFSApp {

    public static void main(String[] args) throws IOException {

        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);

    }
}
