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

    private final static String kuduTableName = "customer-merchant";

    Customer_Merch(KuduClient kuduClient, String hiveConnectionURL){ super(kuduClient, hiveConnectionURL); }

    static int getTransactionVal(KuduClient client, Integer custId, Integer merchId, String colName) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduScanner scanner = client.newScannerBuilder(table)
                .build();

        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                if(result.getInt("MERCHID") == merchId && result.getInt("CUSTID") == custId)
                    return result.getInt(colName);
            }
        }
        return 0;
    }

    static int lastID(KuduClient client, String colName) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduScanner scanner = client.newScannerBuilder(table)
                .build();
        int maxId = 0;
        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                maxId = Math.max(result.getInt(" , merchId,ID"), maxId);
            }
        }
        return maxId;
    }

    static void insertRow(KuduClient client, Integer custId, String custName, Integer merchId, String merchName, Integer transAmt, Integer id) throws KuduException {
        // Open the newly-created table and create a KuduSession.
        KuduTable table = client.openTable(kuduTableName);
        KuduSession session = client.newSession();
        Insert insert = table.newInsert();
        insert.getRow().addInt("ID", id);
        insert.getRow().addInt("CUSTID", custId);
        insert.getRow().addString("CUSTNAME", custName);
        insert.getRow().addInt("MERCHID", merchId);
        insert.getRow().addString("MERCHNAME", merchName);
        insert.getRow().addInt("TRANSAMT", transAmt);
        session.apply(insert);
        session.close();
    }

    static void updateRow(KuduClient client, Integer custId, String custName, Integer merchId, String merchName, Integer transAmt, Integer id) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduSession session = client.newSession();
        Update update = table.newUpdate();
        update.getRow().addInt("ID", id);
        update.getRow().addInt("CUSTID", custId);
        update.getRow().addString("CUSTNAME", custName);
        update.getRow().addInt("MERCHID", merchId);
        update.getRow().addString("MERCHNAME", merchName);
        update.getRow().addInt("TRANSAMT", transAmt);
        session.apply(update);
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

            Integer oldTransAmt = getTransactionVal(kuduClient, custId, merchId, "TRANSAMT");
            Integer kuduId = getTransactionVal(kuduClient, custId, merchId, "ID");
            Integer lastId = lastID(kuduClient, "ID");
            if(oldTransAmt == 0){
                insertRow(kuduClient, custId, custName, merchId, merchName, transAmt, lastId);
            }
            else {
                updateRow(kuduClient, custId, custName, merchId, merchName, (oldTransAmt + transAmt), kuduId);
            }
        }
    }

    @Override
    public void handleDeletion(FlowFile flowFile) throws Exception {
        String databaseName = flowFile.getAttribute("database_name");
        String tableName = flowFile.getAttribute("table_name");
        String keyValue = flowFile.getAttribute("primary_key");
        Integer terminalId, transAmt, cardId, custId, merchId;
        String custName = "", merchName = "";
        if(tableName == "")
    }

    @Override
    public void handleUpdate(FlowFile flowFile) throws Exception {

    }
}
