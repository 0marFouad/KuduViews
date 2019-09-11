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
import java.util.Date;
import java.sql.*;


public class Customer_Merch extends View {

    private final static String kuduTableName = "customer-merchant";

    Customer_Merch(KuduClient kuduClient, String hiveConnectionURL){ super(kuduClient, hiveConnectionURL); }

    static Double getTransactionVal(KuduClient client, Integer custId, Integer merchId, String colName) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduScanner scanner = client.newScannerBuilder(table)
                .build();

        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                if(result.getInt("MERCHID") == merchId && result.getInt("CUSTID") == custId)
                    return result.getDouble(colName);
            }
        }
        return Double.valueOf(0);
    }

    static String getId(KuduClient client, Integer custId, Integer merchId) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduScanner scanner = client.newScannerBuilder(table)
                .build();

        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                if(result.getInt("MERCHID") == merchId && result.getInt("CUSTID") == custId)
                    return result.getString("ID");
            }
        }
        return "";
    }

    static void insertRow(KuduClient client, Integer custId, String custName, Integer merchId, String merchName, Double transAmt, String id) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduSession session = client.newSession();
        Insert insert = table.newInsert();
        insert.getRow().addString("ID", id);
        insert.getRow().addInt("CUSTID", custId);
        insert.getRow().addString("CUSTNAME", custName);
        insert.getRow().addInt("MERCHID", merchId);
        insert.getRow().addString("MERCHNAME", merchName);
        insert.getRow().addDouble("TRANSAMT", transAmt);
        session.apply(insert);
        session.close();
    }

    static void updateRow(KuduClient client, Integer custId, String custName, Integer merchId, String merchName, Double transAmt, String id) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduSession session = client.newSession();
        Update update = table.newUpdate();
        update.getRow().addString("ID", id);
        update.getRow().addInt("CUSTID", custId);
        update.getRow().addString("CUSTNAME", custName);
        update.getRow().addInt("MERCHID", merchId);
        update.getRow().addString("MERCHNAME", merchName);
        update.getRow().addDouble("TRANSAMT", transAmt);
        session.apply(update);
    }

    static void deleteRow(KuduClient client, String id, String colName) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduSession session = client.newSession();
        Delete delete = table.newDelete();
        delete.getRow().addString(colName, id);
        session.apply(delete);
    }

    static void deleteRows(KuduClient client, Integer merchId, Integer custId) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduScanner scanner = client.newScannerBuilder(table)
                .build();
        KuduSession session = client.newSession();
        Delete delete = table.newDelete();
        String id = "";

        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                if(result.getInt("MERCHID") == merchId && result.getInt("CUSTID") == custId)
                {
                    id = result.getString("ID");
                    delete.getRow().addString("ID", id);
                    session.apply(delete);
                }
            }
        }
    }

    @Override
    public void handleInsertion(FlowFile flowFile) throws Exception {
        String databaseName = flowFile.getAttribute("database_name");
        String tableName = flowFile.getAttribute("table_name");
        String keyValue = flowFile.getAttribute("primary_key");
        Integer terminalId, cardId, custId, merchId;
        Double transAmt;
        String custName = "", merchName = "";

        if(tableName.equals("transactions")){

            //set KuduTable
            KuduTable table = kuduClient.openTable(tableName);
            Connection conn = DriverManager.getConnection(hiveConnectionURL + "/" + databaseName, "hdfs", "");

            // Selecting in transactions table
            String query = "select * from transactions where MT_CODE = " + keyValue;
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query);
            rs.next();
            terminalId = rs.getInt("TERM_ID");
            transAmt = Double.valueOf(rs.getInt("TRAN_AMOUNT"));
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

            // Getting old transaction amount from kudu table
            Double oldTransAmt = getTransactionVal(kuduClient, custId, merchId, "TRANSAMT");
            String kuduId = getId(kuduClient, custId, merchId);
            if(oldTransAmt == 0 || kuduId.equals("")){
                Date date= new Date();
                long time = date.getTime();
                String id = Long.toString(time);
                insertRow(kuduClient, custId, custName, merchId, merchName, transAmt, id);
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
        String[] values = flowFile.getAttribute("new_values").split(",");
        Double transAmt = Double.parseDouble(values[1]);
        Integer terminalId, cardId, custId, merchId;
        String custName = "", merchName = "", query = "";

        //set KuduTable
        KuduTable table = kuduClient.openTable(tableName);
        Connection conn = DriverManager.getConnection(hiveConnectionURL + "/" + databaseName, "hdfs", "");

        // Selecting in transactions table
        terminalId = Integer.parseInt(values[2]);
        cardId = Integer.parseInt(values[3]);

        // Selecting from cards table and extracting customer id
        Statement st = conn.createStatement();
        ResultSet rs;
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

        // old transaction amount in record in kudu
        Double oldTransAmt = getTransactionVal(kuduClient, custId, merchId, "TRANSAMT");
        String kuduId = getId(kuduClient, custId, merchId);

        if(tableName.equals("transactions"))
        {
            // Update record with the new transaction amount
            updateRow(kuduClient, custId, custName, merchId, merchName, (oldTransAmt - transAmt), kuduId);
        }
    }

    @Override
    public void handleUpdate(FlowFile flowFile) throws Exception {
        String databaseName = flowFile.getAttribute("database_name");
        String tableName = flowFile.getAttribute("table_name");
        String keyValue = flowFile.getAttribute("primary_key");
        String[] new_values = flowFile.getAttribute("new_values").split(",");
        String[] old_values = flowFile.getAttribute("old_values").split(",");
        Double new_transAmt = Double.parseDouble(new_values[1]);
        Double old_transAmt = Double.parseDouble(old_values[1]);
        Integer terminalId, cardId, custId, merchId;
        String custName = "", merchName = "", query = "";
        String recordDate = new_values[6];
        //set KuduTable
        KuduTable table = kuduClient.openTable(tableName);
        Connection conn = DriverManager.getConnection(hiveConnectionURL + "/" + databaseName, "hdfs", "");

        // Selecting in transactions table
        terminalId = Integer.parseInt(new_values[2]);
        cardId = Integer.parseInt(new_values[3]);

        // Selecting from cards table and extracting customer id
        Statement st = conn.createStatement();
        ResultSet rs;
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

        // Old transaction amount in record in kudu
        Double currentTransAmt = getTransactionVal(kuduClient, custId, merchId, "TRANSAMT");
        Double transAmt = (currentTransAmt - old_transAmt) + new_transAmt;
        // Retreive table id in kudu
        String kuduId = getId(kuduClient, custId, merchId);

        // Selecting from merchants and extracting merchant name
        query = "select * from merchants where id = " + merchId;
        rs = st.executeQuery(query);
        rs.next();
        merchName = rs.getString("name");

        if(tableName == "transactions")
        {
            // Deleting the row
            updateRow(kuduClient, custId, custName, merchId, merchName, transAmt, kuduId);
        }
    }
}
