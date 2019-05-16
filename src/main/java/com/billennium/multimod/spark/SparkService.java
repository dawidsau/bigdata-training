package com.billennium.multimod.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public interface SparkService {

    /**
     * Method responsible for processing data using defined UDF
     * @param schema - base data table format
     * @param dataFrame - Data frame
     * @param udf - user define function
     * @return List with result of precessing
     */
    List<List<String>> processDataWithUDF(StructType schema, Dataset<Row> dataFrame, UDF1 udf);

}
