package scc.processors.demo;

import java.util.List;
import java.util.Random;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.nifi.flowfile.FlowFile;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;


public class Customer_Report extends View {

    Customer_Report(KuduClient kuduClient, String hiveConnectionURL){ super(kuduClient, hiveConnectionURL); }


    static int getTransactionCount(KuduClient client, String tableName, int terminalId) throws KuduException {
        KuduTable table = client.openTable(tableName);

        KuduScanner scanner = client.newScannerBuilder(table)
                .build();
//
//        while (scanner.hasMoreRows()) {
//            RowResultIterator results = scanner.nextRows();
//            while (results.hasNext()) {
//                RowResult result = results.next();
//                if(result.getInt(0) == terminalId)
//                    return result.getInt(1);
//            }
//        }
        return 0;
    }

    static void updateRow(KuduClient client, String tableName, int keyValue, int newTransCount) throws KuduException {
        KuduTable table = client.openTable(tableName);
        KuduSession session = client.newSession();
//        Update update = table.newUpdate();
//        update.getRow().addInt("TERMID", keyValue);
//        update.getRow().addInt("TRANSCOUNT", newTransCount);
//        session.apply(update);
    }

    static void insertRow(KuduClient client, String tableName, int keyValue) throws KuduException {
        // Open the newly-created table and create a KuduSession.
//        KuduTable table = client.openTable(tableName);
//        KuduSession session = client.newSession();
//        Insert insert = table.newInsert();
//        insert.getRow().addInt("TERMID", keyValue);
//        insert.getRow().addInt("TRANSCOUNT", 1);
//        session.apply(insert);
//        session.close();
    }

    @Override
    public void handleInsertion(FlowFile flowFile) throws Exception {

    }

    @Override
    public void handleDeletion() {

    }

    @Override
    public void handleUpdate() {

    }
}
