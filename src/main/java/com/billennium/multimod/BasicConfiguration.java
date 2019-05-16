package com.billennium.multimod;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


public class BasicConfiguration extends Configuration {

    private static final String DEFAULT_FS = "fs.defaultFS";
    private static final String CONF_VALUE = "hdfs://billhdp01.training.dev:8020";

    public static Configuration getHDFSConfig() {
        Configuration config = new BasicConfiguration();
        config.get(DEFAULT_FS, CONF_VALUE);
        config.addResource(new Path("/etc/hbase/conf", "hbase-site.xml"));
        config.addResource(new Path("/etc/hadoop/conf", "core-site.xml"));
//        config.addResource(new Path("/etc/spark2/conf", "spark-defaults.conf"));
        return config;
    }


}
