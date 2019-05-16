package com.billennium.multimod.spark;

import org.apache.spark.sql.SparkSession;

public class SparkConfig {

    private static SparkSession sparkSession;

    /**
     * Spark session creation method
     *
     * @param environment
     * @param appName
     * @return SparkSession
     */
    public synchronized static SparkSession getSparkSession(String environment, String appName) {
        if (sparkSession == null) {
            sparkSession = SparkSession
                    .builder()
                    .master(environment)
                    .appName(appName)
                    .getOrCreate();
        }
        return sparkSession;
    }

    /**
     * Session closing method
     */
    static void closeSparkSession() {
        if (sparkSession != null) {
            sparkSession.close();
        }
    }

}
