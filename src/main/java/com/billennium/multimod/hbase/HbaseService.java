package com.billennium.multimod.hbase;

import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface HbaseService {


    /**
     * Use this method after "getConnection" to avoid exceptions and remember to use "closeConnection" when job is done.
     *
     * @param namespace
     * @param tableName
     * @param columnFamily
     * @param data
     */
    void savePutDataToTable(String namespace, String tableName, String columnFamily, Map<String, Map<String, String>> data);

    /**
     * Method responsible for creation new table in HBase environment
     *
     * @param tableName    - table name should contains namespace as well
     * @param columnFamily
     * @return New instance of table or table with the same tableName
     * @throws IOException
     */
    Table createTable(String tableName, List<String> columnFamily) throws IOException;

}
