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


public class Customer_Merch extends View {

    Customer_Merch(KuduClient kuduClient, String hiveConnectionURL){ super(kuduClient, hiveConnectionURL); }

    static int getTransactionCount(KuduClient client, String tableName, int terminalId) throws KuduException {
        KuduTable table = client.openTable(tableName);

        KuduScanner scanner = client.newScannerBuilder(table)
                .build();
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

    static void insertRow(KuduClient client, String tableName, Integer custId, String custName, Integer merchId, String merchName, Integer transAmt) throws KuduException {
        // Open the newly-created table and create a KuduSession.
        KuduTable table = client.openTable(tableName);
        KuduSession session = client.newSession();
        Insert insert = table.newInsert();
        insert.getRow().addInt("CUSTID", custId);
        insert.getRow().addString("CUSTNAME", custName);
        insert.getRow().addInt("MERCHID", merchId);
        insert.getRow().addString("MERCHNAME", merchName);
        insert.getRow().addInt("TRANSAMT", transAmt);
        session.apply(insert);
        session.close();
    }

    @Override
    public void handleInsertion(FlowFile flowFile) throws Exception {
        String databaseName = flowFile.getAttribute("database_name");
        String tableName = flowFile.getAttribute("table_name");
        String keyValue = flowFile.getAttribute("primary_key");
        Integer terminalId, transAmt, cardId, custId, merchId;
        String custName = "", merchName = "";
        if(tableName == "transactions"){
            //set KuduTable
            KuduTable table = kuduClient.openTable(tableName);
            Schema schema = table.getSchema();
            Connection conn = DriverManager.getConnection(hiveConnectionURL + "/" + databaseName, "hdfs", "");

            // Selecting in transactions table
            String query = "select * from transactions where MT_CODE = " + keyValue;
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query);
            rs.next();
            terminalId = rs.getInt("TERM_ID");
            transAmt = rs.getInt("TRAN_AMOUNT");
            cardId = rs.getInt("CARD_ID");

            // Selecting from cards table and extracting customer id
            query = "select * from cards where id = " + cardId;
            rs = st.executeQuery(query);
            rs.next();
            custId = rs.getInt("cust_id");

            // Selecting from cards table and extracting customer name
            query = "select * from customers where id = " + custId;
            rs = st.executeQuery(query);
            rs.next();
            custName = rs.getString("name");

            // Selecting from terminals and extracting merchant id
            query = "select * from terminals where id = " + terminalId;
            rs = st.executeQuery(query);
            rs.next();
            merchId = rs.getInt("merch_id");

            // Selecting from merchants and extracting merchant name
            query = "select * from merchants where id = " + merchId;
            rs = st.executeQuery(query);
            rs.next();
            merchName = rs.getString("name");

            // Insert the row
            Integer oldTransAmt = getTransactionCount(kuduClient, tableName, terminalId);
            insertRow(kuduClient, tableName, custId, custName, merchId, merchName, transAmt);

        }
    }

    @Override
    public void handleDeletion() {

    }

    @Override
    public void handleUpdate() {

    }
}
