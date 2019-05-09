package com.billennium.training.exercise2;

import au.com.bytecode.opencsv.CSVParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static com.billennium.training.exercise3.TwitterAuthorsCounterExercise.jobAndHDFSConfiguration;

public class TwitterAuthorsCounter {
    public static class AuthorRetriveMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            CSVParser parser = new CSVParser();
            String[] fields = parser.parseLineMulti(value.toString());
            word.set(fields[4]);
            context.write(word, one);
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        jobAndHDFSConfiguration(args);

    }
}