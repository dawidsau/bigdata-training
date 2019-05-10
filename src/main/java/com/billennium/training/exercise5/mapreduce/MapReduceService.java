package com.billennium.training.exercise5.mapreduce;

import java.io.IOException;

public interface MapReduceService {

    void countUsersTwits(String csvPath, String outputPath)
            throws IOException, ClassNotFoundException, InterruptedException;

}
