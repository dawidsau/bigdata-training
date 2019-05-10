package com.billennium.training.exercise5;

import com.billennium.training.exercise5.hbase.BasicHBaseService;
import com.billennium.training.exercise5.hbase.HBaseService;
import com.billennium.training.exercise5.hdfs.BasicHdfsWorker;
import com.billennium.training.exercise5.hdfs.HdfsWorker;
import com.billennium.training.exercise5.mapreduce.MapReduceService;
import com.billennium.training.exercise5.mapreduce.TwitterAuthorCounter;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class MultiModExercise {


    private HBaseService hBaseService = new BasicHBaseService();
    private HdfsWorker hdfsWorker = new BasicHdfsWorker();
    private MapReduceService mapReduceService = new TwitterAuthorCounter();


    public void init() throws IOException, InterruptedException, ClassNotFoundException {
        hdfsWorker.copyFileFromLocalToHDFS("/training/twitter/testdata.manual.2009.06.14.csv", "/tmp/twitter");
        hBaseService.putCsvRecords("/training/twitter/testdata.manual.2009.06.14.csv", "dsauermann", "twitter");
        mapReduceService.countUsersTwits("/tmp/twitter/*", "/user/dsauermann/mr_outputs/twitter/");
        hBaseService.createNewTable("dsauermann:result", Arrays.asList("value"));
        File file = BaseConfiguration.getHDFSConfig().getFile("/user/dsauermann/mr_outputs/twitter", "part-r-00000");
        hBaseService.putMapReduceResult(file, "dsauermann", "result");
    }


    public static void main(String[] args) {
        MultiModExercise exercise = new MultiModExercise();
        try {
            exercise.init();
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

}

