package com.billennium.training.exercise5.hdfs;

import com.billennium.training.exercise5.BaseConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;


@Slf4j
public class BasicHdfsWorker implements HdfsWorker {

    @Override
    public void copyFileFromHDFSToLocal(String actual, String destination) throws IOException {
        FileSystem fs = FileSystem.get(BaseConfiguration.getHDFSConfig());
        fs.copyToLocalFile(new Path(actual), new Path(destination));
        log.debug("File copied to: " + destination);
    }

    @Override
    public void copyFileFromLocalToHDFS(String actual, String destination) throws IOException {
        FileSystem fs = FileSystem.get(BaseConfiguration.getHDFSConfig());
        fs.copyFromLocalFile(new Path(actual), new Path(destination));
        log.debug("File copied to: " + destination);

    }
}
