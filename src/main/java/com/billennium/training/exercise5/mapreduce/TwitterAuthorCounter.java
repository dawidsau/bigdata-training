package com.billennium.training.exercise5.mapreduce;

import au.com.bytecode.opencsv.CSVParser;
import com.billennium.training.exercise2.TwitterAuthorsCounter;
import com.billennium.training.exercise5.BaseConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

@Slf4j
public class TwitterAuthorCounter implements MapReduceService {


    @Override
    public void countUsersTwits(String csvPath, String outputPath) throws IOException,
            ClassNotFoundException, InterruptedException {

        Configuration conf = BaseConfiguration.getHDFSConfig();
        Job job = Job.getInstance(conf, "Calculation Users Twits");
        job.setJarByClass(TwitterAuthorsCounter.class);
        job.setMapperClass(AuthorRetrieveMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(csvPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);
        log.debug("Map reduce job done");

    }


    public static class AuthorRetrieveMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            CSVParser parser = new CSVParser();
            String[] fields = parser.parseLineMulti(value.toString());
            word.set(fields[4]);
            context.write(word, ONE);
            log.debug("Mapping job done");
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Integer sum = 0;
            for (IntWritable value : values) {
                sum = sum + value.get();
            }
            result.set(sum);
            context.write(key, result);
            log.debug("Reducer job done");
        }
    }
}
