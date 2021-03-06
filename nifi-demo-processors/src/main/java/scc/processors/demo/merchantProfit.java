package scc.processors.demo;

import com.mysql.cj.xdevapi.DatabaseObject;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannelOutputStream;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;

import static scc.processors.demo.merchantProfit.*;


public class merchantProfit extends View {


    private final static String kuduTableName = "merchantprofit";

    private  String id;
    merchantProfit(KuduClient kuduClient, String hiveConnectionURL) {
        super(kuduClient, hiveConnectionURL);
    }


    @Override
    public void handleDeletion(FlowFile flowFile) throws Exception {




        String databaseName = flowFile.getAttribute("database_name");
        String tableName = flowFile.getAttribute("table_name");
        String[] new_values = flowFile.getAttribute("new_values").split(",");
        String temp = new_values[0].substring(1,new_values[0].length());

        int MT_CODE = Integer.parseInt(temp);

        KuduTable table = kuduClient.openTable(kuduTableName);
        Schema schema = table.getSchema();

        //get Terminal_ID from Hive
        int merchantId;
        int terminalId;
        String merchantName;
        int transaction_amount;
        System.out.println("hussein");
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection(hiveConnectionURL + "/" + databaseName, "hdfs", "");
        System.out.println(new_values[0]);

        if(tableName.equals("transactions")){
            System.out.println("is the error here 0 ");

        // get terminal id & transactions_amount from transactions

            terminalId = Integer.parseInt(new_values[2]);
            transaction_amount = Integer.parseInt(new_values[1]);

        // get merch-id from terminals;

            System.out.println("is the error here 1 ");

            String query = "select * from terminals where id = " + terminalId;
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query);
        System.out.println(rs);
        rs.next();
        merchantId = rs.getInt("merch_id");


        //get merchant name from merchants

        query = "select * from merchants where id = " + merchantId;
        st = conn.createStatement();
        rs = st.executeQuery(query);
        System.out.println(rs);
        rs.next();
        merchantName = rs.getString("name");


             int transactionCount = getTransactionCount(kuduClient, kuduTableName, merchantId);
            System.out.println("is the error here2 ");

            Double total_transaction_amount = getTransactionAmount(kuduClient, kuduTableName, merchantId);
            System.out.println("is the error here 3");

            total_transaction_amount = total_transaction_amount - transaction_amount;
            transactionCount = transactionCount - 1;
            if (transactionCount == 0) {

                KuduSession session = kuduClient.newSession();
                Delete delete = table.newDelete();
                delete.getRow().addString("ID", id);
                session.apply(delete);
                session.close();
            } else if (transactionCount == -1) {
                // do nothing
            }else{
                    updateRow(kuduClient, merchantId, merchantName, transaction_amount + total_transaction_amount, transactionCount );
                }

        } else {
            // this means the delete is in merchants

            int merch_id = Integer.parseInt(new_values[1]);
            KuduSession session = kuduClient.newSession();
            Delete delete = table.newDelete();
            delete.getRow().addInt("ID", merch_id);
            session.apply(delete);
            session.close();

        }
        }

    @Override
    public void handleInsertion(FlowFile flowFile) throws Exception {

        String databaseName = flowFile.getAttribute("database_name");
        String tableName = flowFile.getAttribute("table_name");
        String[] new_values = flowFile.getAttribute("new_values").split(",");
        String temp = new_values[0].substring(1,new_values[0].length());

        int MT_CODE = Integer.parseInt(temp);

        KuduTable table = kuduClient.openTable(kuduTableName);
        Schema schema = table.getSchema();

        //get Terminal_ID from Hive
        int merchantId;
        int terminalId;
        String merchantName;
        double transaction_amount;
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection(hiveConnectionURL + "/" + databaseName, "hdfs", "");



        // get terminal id & transactions_amount from transactions
        String query = "select * from transactions where MT_CODE = " + MT_CODE;
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(query);
        System.out.println(rs);
        rs.next();
        System.out.println("opaaa1");
        terminalId = rs.getInt("term_id");
        System.out.println("opaaa2");
        transaction_amount = rs.getInt("tran_amount");
        System.out.println("opaaa3");

        // get merch-id from terminals;


        query = "select * from terminals where id = " + terminalId;
        st = conn.createStatement();
        rs = st.executeQuery(query);
        System.out.println(rs);
        rs.next();
        System.out.println("opaaa4");
        merchantId = rs.getInt("merch_id");
        System.out.println("opaaa5");


        //get merchant name from merchants

        query = "select * from merchants where id = " + merchantId;
        st = conn.createStatement();
        rs = st.executeQuery(query);
        System.out.println(rs);
        rs.next();
        merchantName = rs.getString("name");




        int transactionCount = getTransactionCount(kuduClient,kuduTableName,merchantId);

        double total_transaction_amount = getTransactionAmount(kuduClient,kuduTableName,merchantId);

        //Create new statement with inserting in kudu Number of transactions + 1
        if(transactionCount == 0){
            insertRow(kuduClient,merchantId,merchantName,transaction_amount);
        }else{
            updateRow(kuduClient,merchantId,merchantName ,transaction_amount+total_transaction_amount,transactionCount+1);
        }








    }




    private Double getTransactionAmount(KuduClient client, String tableName, Integer merchant_id)  throws KuduException {

        KuduTable table = client.openTable(tableName);

        KuduScanner scanner = client.newScannerBuilder(table)
                .build();

        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                if(result.getInt(1) == merchant_id)
                    return result.getDouble("TRANS_AMT");
            }
        }
        return 0.0;
    }
    private int getTransactionCount(KuduClient client, String tableName, int merchant_id) throws KuduException {
        KuduTable table = client.openTable(tableName);

        KuduScanner scanner = client.newScannerBuilder(table)
                .build();

        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                if(result.getInt(1) == merchant_id){
                    id = result.getString(0);
                    return result.getInt("TRANSCOUNT");}
            }
        }
        return 0;
    }

    static void insertRow(KuduClient client, int merchant_id,String merchant_name ,double transaction_amount) throws KuduException {
        // Open the newly-created table and create a KuduSession.
        KuduTable table = client.openTable(kuduTableName);
        KuduSession session = client.newSession();
        Insert insert = table.newInsert();
        Date date= new Date();
        Long time = date.getTime();
        insert.getRow().addString("ID",String.valueOf(time));
        insert.getRow().addInt("MERCH_ID", merchant_id);
        insert.getRow().addString("MERCH_NAME", merchant_name);
        insert.getRow().addInt("TRANSCOUNT", 1);
        insert.getRow().addDouble("TRANS_AMT", transaction_amount);
        session.apply(insert);
        session.close();
    }
    private void updateRow(KuduClient client,Integer merchantId,String merchant_name  ,double transaction_amount , Integer transaction_count) throws KuduException {

        System.out.println("ahmed heshahm " +  id +" "+transaction_amount +" "+ merchantId +" "+transaction_count +" "+ merchant_name);
        System.out.println("adsakdsahdksahdkhsdkhdkshadkhs");
        KuduTable table = client.openTable(kuduTableName);
        KuduSession session = client.newSession();
        Update update = table.newUpdate();
        update.getRow().addString("ID",id);
        update.getRow().addInt("MERCH_ID", merchantId);
        update.getRow().addString("MERCH_NAME", merchant_name);
        update.getRow().addInt("TRANSCOUNT", transaction_count);
        update.getRow().addDouble("TRANS_AMT", transaction_amount);
        session.apply(update);
        session.close();

    }


    @Override
    public void handleUpdate(FlowFile flowFile) throws Exception {

        String databaseName = flowFile.getAttribute("database_name");
        String tableName = flowFile.getAttribute("table_name");
        String[] new_values = flowFile.getAttribute("new_values").split(",");
        String[] old_values = flowFile.getAttribute("old_values").split(",");

           String temp = new_values[0].substring(1,new_values[0].length());
            int MT_CODE = Integer.parseInt(temp);

            KuduTable table = kuduClient.openTable(kuduTableName);
            Schema schema = table.getSchema();


        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection(hiveConnectionURL + "/" + databaseName, "hdfs", "");

        if(tableName.equals("transactions")){

            int merchantId;
            int terminalId;
            String merchantName;
            double transaction_amount;

            // get terminal id & transactions_amount from transactions
            String query = "select * from transactions where MT_CODE = " + MT_CODE;
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query);
            System.out.println(rs);
            rs.next();
            terminalId = rs.getInt("term_id");
            transaction_amount = rs.getDouble("tran_amount");

            // get merch-id from terminals;


            query = "select * from terminals where id = " + terminalId;
            st = conn.createStatement();
            rs = st.executeQuery(query);
            System.out.println(rs);
            rs.next();
            merchantId = rs.getInt("merch_id");


            //get merchant name from merchants

            query = "select * from merchants where id = " + merchantId;
            st = conn.createStatement();
            rs = st.executeQuery(query);
            System.out.println(rs);
            rs.next();
            merchantName = rs.getString("name");


            int transactionCount = getTransactionCount(kuduClient, kuduTableName, merchantId);
            double total_transaction_amount = getTransactionAmount(kuduClient, kuduTableName, merchantId);

            //Create new statement with inserting in kudu Number of transactions + 1

            updateRow(kuduClient,merchantId,merchantName ,transaction_amount+total_transaction_amount,transactionCount);

        }else {
          // this mean the update happen in merchants
/*

            int merch_id = Integer.parseInt(new_values[0]);

            String new_merch_name = new_values[1];
            String old_merch_name = old_values[1];
            if ( new_merch_name != old_merch_name){


                KuduSession session = kuduClient.newSession();
                Update update = table.newUpdate();
                update.getRow().addInt("MERCH_ID", merch_id);
                update.getRow().addString("MERCH_NAME", new_merch_name);
                session.apply(update);
                session.close();
*/


            }

        }





    }

