package com.billennium.training.exercise5.hbase;

import java.io.File;
import java.io.IOException;
import java.util.List;

public interface HBaseService {

    void putCsvRecords(String path, String namespace, String tableName) throws IOException;

    void putMapReduceResult(File file, String namespace, String tableName, String columnFamily) throws IOException;

    void scanAllRecords(String table, String namespace);

    void createNewTable(String tableName, List<String> columnFamily) throws IOException;

}
