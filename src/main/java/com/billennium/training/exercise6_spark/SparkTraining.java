package com.billennium.training.exercise6_spark;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

public class SparkTraining {

    public void init() {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("test")
                .config("spark.sql.warehouse.dir", "C:\\Users\\dsauermann\\IdeaProjects\\exercise2")
                .getOrCreate();
        System.out.println();
        StructType schema = new StructType()
                .add("name", "string")
                .add("age", "int");
        Dataset<Row> testDF = sparkSession.read()
                .schema(schema)
                .csv("data.csv");
        testDF.show();

        UDF1<Integer, Integer> firstUDF = v -> v * 2;
        sparkSession.udf()
                .register("First_udf", firstUDF, DataTypes.IntegerType);
        Dataset<Row> doubleAgeFrame = testDF.withColumn("doubleAga",
                callUDF("First_udf", col("age")));

        doubleAgeFrame.show();
        sparkSession.close();

    }

    public static void main(String[] args) {
        SparkTraining sparkTraining = new SparkTraining();
        sparkTraining.init();
    }
}
