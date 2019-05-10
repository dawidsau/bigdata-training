package com.billennium.training.exercise5.hdfs;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public interface HdfsWorker {

    void copyFileFromHDFSToLocal(String actual, String destination) throws IOException;

    void copyFileFromLocalToHDFS(String actual, String destination) throws IOException;

}
