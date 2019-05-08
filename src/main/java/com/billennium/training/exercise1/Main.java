package com.billennium.training.exercise1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Main {

    public static void main(String[] args) {

        Path localSrc = new Path(args[0]);
        Path dst = new Path(args[1]);
        Configuration conf = new Configuration();
        System.out.println("Connecting to -- " + conf.get("fs.defaultFS",
                "hdfs://billhdp01.training.dev:8020"));
        try {
            FileSystem fs = FileSystem.get(conf);
            fs.copyFromLocalFile(localSrc, dst);
            System.out.println(dst + " copied to HDFS");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}