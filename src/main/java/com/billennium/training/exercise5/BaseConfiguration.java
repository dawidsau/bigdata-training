package com.billennium.training.exercise5;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


public class BaseConfiguration {

    private static final String DEFAULT_FS = "fs.defaultFS";
    private static final String CONF_VALUE = "hdfs://billhdp01.training.dev:8020";

    public static Configuration getHDFSConfig() {
        Configuration config = new Configuration();
        config.get(DEFAULT_FS, CONF_VALUE);
        config.addResource(new Path("/etc/hbase/conf", "hbase-site.xml"));
        config.addResource(new Path("/etc/hadoop/conf", "core-site.xml"));
        return config;
    }

}
