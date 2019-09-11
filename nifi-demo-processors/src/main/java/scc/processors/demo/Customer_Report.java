package scc.processors.demo;

import java.util.Date;
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

    private final static String kuduTableName = "customer-report";

    Customer_Report(KuduClient kuduClient, String hiveConnectionURL){ super(kuduClient, hiveConnectionURL); }

    static int getTransactionCount(KuduClient client, int id, String recordDate) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduScanner scanner = client.newScannerBuilder(table)
                .build();

        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                if(result.getInt("CUSTID") == id && result.getString("TIME").equals(recordDate))
                    return result.getInt("TRANSCOUNT");
            }
        }
        return 0;
    }

    static Double getTransactionVal(KuduClient client, Integer custId, String recordDate, String colName) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduScanner scanner = client.newScannerBuilder(table)
                .build();

        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                if(result.getString("TIME").equals(recordDate) && result.getInt("CUSTID") == custId)
                    return result.getDouble(colName);
            }
        }
        return Double.valueOf(0);
    }

    static String getId(KuduClient client, Integer custId, String recordDate) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduScanner scanner = client.newScannerBuilder(table)
                .build();

        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                if(result.getInt("CUSTID") == custId && result.getString("TIME").equals(recordDate))
                    return result.getString("ID");
            }
        }
        return "";
    }

    static void updateRow(KuduClient client, Integer custId, String custName, Double transAmt, Integer transCount, String recordDate, String id) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduSession session = client.newSession();
        Update update = table.newUpdate();
        update.getRow().addString("ID", id);
        update.getRow().addInt("CUSTID", custId);
        update.getRow().addString("CUSTNAME", custName);
        update.getRow().addDouble("TRANSAMT", transAmt);
        update.getRow().addInt("TRANSCOUNT", transCount);
        update.getRow().addString("TIME", recordDate);
        session.apply(update);
    }

    static void insertRow(KuduClient client, Integer custId, String custName, String recordDate, Integer transCount, Double transAmt, String id) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduSession session = client.newSession();
        Insert insert = table.newInsert();
        insert.getRow().addString("ID", id);
        insert.getRow().addInt("CUSTID", custId);
        insert.getRow().addString("CUSTNAME", custName);
        insert.getRow().addDouble("TRANSAMT", transAmt);
        insert.getRow().addInt("TRANSCOUNT", transCount);
        insert.getRow().addString("TIME", recordDate);
        session.apply(insert);
        session.close();
    }

    static void deleteRow(KuduClient client, String id, String colName) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduSession session = client.newSession();
        Delete delete = table.newDelete();
        delete.getRow().addString(colName, id);
        session.apply(delete);
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
            String recordDate = (rs.getTimestamp("RECORD_DATE").getMonth() + 1)+ "-" + (rs.getTimestamp("RECORD_DATE").getYear() + 1900);
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

            Double oldTransAmt = getTransactionVal(kuduClient, custId, recordDate, "TRANSAMT");
            Integer transactionCount = getTransactionCount(kuduClient, custId, recordDate);
            String kuduId = getId(kuduClient, custId, recordDate);
            if(oldTransAmt == 0 || kuduId.equals("")){
                java.util.Date date= new Date();
                long time = date.getTime();
                String id = Long.toString(time);
                insertRow(kuduClient, custId, custName, recordDate, transactionCount+1, transAmt, id);
            }
            else {
                updateRow(kuduClient, custId, custName, (oldTransAmt + transAmt), transactionCount+1, recordDate, kuduId);
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
        String recordDate = values[6];
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

        // Old transaction amount in record in kudu
        Double oldTransAmt = getTransactionVal(kuduClient, custId, recordDate, "TRANSAMT");
        // Retreive table id in kudu
        String kuduId = getId(kuduClient, custId, recordDate);
        //
        int transactionCount = getTransactionCount(kuduClient, custId, recordDate);

        // Selecting from merchants and extracting merchant name
        query = "select * from merchants where id = " + merchId;
        rs = st.executeQuery(query);
        rs.next();
        merchName = rs.getString("name");

        if(tableName == "transactions")
        {
            // Deleting the row
            updateRow(kuduClient, custId, custName, (oldTransAmt - transAmt), transactionCount-1, recordDate, kuduId);
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
        Double currentTransAmt = getTransactionVal(kuduClient, custId, recordDate, "TRANSAMT");
        Double transAmt = (currentTransAmt - old_transAmt) + new_transAmt;
        // Retreive table id in kudu
        String kuduId = getId(kuduClient, custId, recordDate);
        //
        int transactionCount = getTransactionCount(kuduClient, custId, recordDate);

        // Selecting from merchants and extracting merchant name
        query = "select * from merchants where id = " + merchId;
        rs = st.executeQuery(query);
        rs.next();
        merchName = rs.getString("name");

        if(tableName == "transactions")
        {
            // Deleting the row
            updateRow(kuduClient, custId, custName, transAmt, transactionCount, recordDate, kuduId);
        }
    }
}
