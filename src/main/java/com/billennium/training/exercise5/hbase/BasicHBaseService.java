package com.billennium.training.exercise5.hbase;

import au.com.bytecode.opencsv.CSVReader;
import com.billennium.training.exercise5.BaseConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class BasicHBaseService implements HBaseService {


    @Override
    public void putCsvRecords(String path, String namespace, String tableName) throws IOException {
        FileReader fileReader = new FileReader(path);
        CSVReader reader = new CSVReader(fileReader);
        List<String[]> recordList = reader.readAll();

        Map<String, String> inputMap = new HashMap<>();

        recordList.stream()
                .forEach(r -> {
                            inputMap.put("id", r[1]);
                            inputMap.put("polarity", r[0]);
                            inputMap.put("date", r[2]);
                            inputMap.put("query", r[3]);
                            inputMap.put("user", r[4]);
                            inputMap.put("value", r[5]);
                            try {
                                insertData(BaseConfiguration.getHDFSConfig(), namespace, tableName, r[1], "text", inputMap);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                );
        log.debug("CSV input saved on HBase table. Namespace:  " + namespace + ", tableName: " + tableName + ".");
    }

    @Override
    public void putMapReduceResult(File file, String namespace, String tableName) throws IOException {
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        Map<String, String> result = new HashMap<>();
        String line = bufferedReader.readLine();
        int iterable = 1;
        while (line != null) {
            String[] lineValues = line.split(" ");
            result.put(lineValues[0], lineValues[1]);
            insertData(BaseConfiguration.getHDFSConfig(), namespace, tableName, "" + iterable++, "text", result);

            line = bufferedReader.readLine();
        }
        log.debug("Result data inserted into HBase table.");
    }

    //TODO Scan implementation
    @Override
    public void scanAllRecords(String table, String namespace) {

    }


    //TODO if exists not need to add or replace
    @Override
    public void createNewTable(String tableName, List<String> columnFamily) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(BaseConfiguration.getHDFSConfig());
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        columnFamily.forEach(s ->
                tableDescriptor.addFamily(new HColumnDescriptor(s))
        );
        admin.createTable(tableDescriptor);
        log.debug("Result table added.");

    }

    public void insertData(Configuration config, String namespace, String tableName, String rowkey, String columnFamily, Map<String, String> data) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Put put = new Put(Bytes.toBytes(rowkey));
            data.forEach((k, v) ->
                    put.addImmutable(Bytes.toBytes(columnFamily), Bytes.toBytes(k), Bytes.toBytes(v))
            );
            TableName tn = TableName.valueOf(namespace, tableName);
            Table table = connection.getTable(tn);
            table.put(put);
        }
    }
}
