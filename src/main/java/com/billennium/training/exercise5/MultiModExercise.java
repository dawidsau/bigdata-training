package com.billennium.training.exercise5;

import com.billennium.training.exercise5.hbase.BasicHBaseService;
import com.billennium.training.exercise5.hbase.HBaseService;
import com.billennium.training.exercise5.hdfs.BasicHdfsWorker;
import com.billennium.training.exercise5.hdfs.HdfsWorker;
import com.billennium.training.exercise5.mapreduce.MapReduceService;
import com.billennium.training.exercise5.mapreduce.TwitterAuthorCounter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

@Slf4j
public class MultiModExercise {


    private HBaseService hBaseService = new BasicHBaseService();
    private HdfsWorker hdfsWorker = new BasicHdfsWorker();
    private MapReduceService mapReduceService = new TwitterAuthorCounter();


    public void init() throws IOException {
        hdfsWorker.copyFileFromLocalToHDFS("/training/twitter/testdata.manual.2009.06.14.csv", "/tmp/twitter");
        hBaseService.putCsvRecords("/training/twitter/testdata.manual.2009.06.14.csv", "dsauermann", "twitter");
        try {
            hBaseService.createNewTable("dsauermann:result", Arrays.asList("value"));
        } catch (IOException e) {
            e.printStackTrace();
            log.warn("HBase table already created");
        }
        try {
            mapReduceService.countUsersTwits("/tmp/twitter/*", "/user/dsauermann/mr_outputs/twitter/");
        } catch (InterruptedException | IOException | ClassNotFoundException e) {
            e.printStackTrace();
            log.warn("Map reduce method issued");
        }
        hdfsWorker.copyFileFromHDFSToLocal("/user/dsauermann/mr_outputs/twitter/part-r-00000","/home/dsauermann/twitter/part-r-00000");
        File file = new File("/home/dsauermann/twitter/part-r-00000");
        hBaseService.putMapReduceResult(file, "dsauermann", "result", "value");
//        hBaseService.scanAllRecords("result","dsauermann");
    }


    public static void main(String[] args) {
        MultiModExercise exercise = new MultiModExercise();
        try {
            exercise.init();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

