package com.billennium.training.exercise4;

import au.com.bytecode.opencsv.CSVReader;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Slf4j
@Getter
public class HBaseExercise {

    private Configuration config;
    private String tableName;
    private String namespace;

    public HBaseExercise(String tableName, String namespace) {
        this.namespace = namespace;
        this.tableName = tableName;
        config = HBaseConfiguration.create();
        config.addResource(new Path("/etc/hbase/conf", "hbase-site.xml"));
        config.addResource(new Path("/etc/hadoop/conf", "core-site.xml"));
    }

    public void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
            System.out.println("Removed existing table [" + table.getNameAsString() +
                    "]");
        }
        admin.createTable(table);
        System.out.println("Table created [" + table.getNameAsString() + "]");
    }

    public void createNamespace(Admin admin, String namespace) throws IOException {
        NamespaceDescriptor desc = admin.getNamespaceDescriptor(namespace);
        if (desc == null) {
            desc = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(desc);
        }
    }

    public void createSchemaTables(Configuration config, String namespace, String tableName, List<String> columnFamilies) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin()) {
            createNamespace(admin, namespace);
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(namespace, tableName));
            columnFamilies.forEach(c -> {
                table.addFamily(new HColumnDescriptor(c).setCompressionType(Compression.Algorithm.NONE));
            });
            System.out.print("Creating table. ");
            createOrOverwrite(admin, table);
            System.out.println(" Done.");
        }
    }

    public void insertData(Configuration config, String namespace, String tableName, String rowkey, String columnFamily, Map<String, String> data) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Put p = new Put(Bytes.toBytes(rowkey));
            data.forEach((col, v) ->
                    p.addImmutable(Bytes.toBytes(columnFamily), Bytes.toBytes(col), Bytes.toBytes(v))
            );
            Table table = connection.getTable(TableName.valueOf(namespace, tableName));
            table.put(p);
            table.close();
        }
    }

    public void findAndPrint(Configuration config, String namespace, String tableName, String keyPrefix, String cf, String qualifier, String value, String qualifierToPrint, int limit) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Scan scanner = new Scan();
            FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filters.addFilter(new PrefixFilter(Bytes.toBytes(keyPrefix)));
            filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes(cf), Bytes.toBytes(qualifier), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value)));
            filters.addFilter(new PageFilter(limit));
            scanner.setFilter(filters);
            scanner.setCaching(limit);
            Table table = connection.getTable(TableName.valueOf(namespace, tableName));
            ResultScanner results = table.getScanner(scanner);
            Iterator<Result> iter = results.iterator();
            int count = 0;
            log.info("Scanning table [" + tableName + "]");
            log.info("Results:");
            while (iter.hasNext() && count < limit) {
                count++;
                Result r = iter.next();
                System.out.println(Bytes.toString(r.getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifierToPrint))));
            }
            table.close();
        }
    }

    private void insertDataToHBaseFromCSV(String csvPath) throws IOException {
        FileReader fileReader = new FileReader(new Path(csvPath).toString());
        CSVReader reader = new CSVReader(fileReader);
        List<String[]> allValues = reader.readAll();

        Map<String, String> data = new HashMap<>();
        allValues.stream()
                .forEach(f -> {
                    data.put("polarity", f[0]);
                    data.put("id", f[1]);
                    data.put("date", f[2]);
                    data.put("query", f[3]);
                    data.put("user", f[4]);
                    data.put("value", f[5]);
                    try {
                        insertData(config, namespace, tableName, f[1], "text", data);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    data.clear();
                });
    }

    public static void main(String[] args) throws Exception {
        HBaseExercise exercise = new HBaseExercise("twitter", "dsauermann");
        exercise.insertDataToHBaseFromCSV(args[0]);
        log.info("Job done");
        System.exit(0);
    }
}
