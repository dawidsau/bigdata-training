package com.billennium.multimod.hbase;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

@Slf4j
public class HBaseConfig {

    private static Connection con;

    /**
     * First step to connect with HBase
     *
     * @param config Hadoop config
     * @return Hbase connection || null if generating connection was interrupted
     */
    public static synchronized Connection getConnection(Configuration config) {
        if (con == null) {
            try {
                con = ConnectionFactory.createConnection(config);
            } catch (IOException e) {
                log.error("<----------->Could not create connection<------------>");
            }
        }
        return con;
    }

    /**
     * Connection closing method
     */
    public void closeConnection() {
        if (con != null) {
            try {
                con.close();
            } catch (IOException e) {
                log.error("Hbase could not be closed");
            }
        }
    }
}
