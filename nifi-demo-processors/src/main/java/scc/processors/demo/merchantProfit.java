package scc.processors.demo;

import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.nifi.flowfile.FlowFile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;

import static scc.processors.demo.merchantProfit.*;


public class merchantProfit extends View {


    private final static String kuduTableName = "views::merchant-profit";

    private  String id;
    merchantProfit(KuduClient kuduClient, String hiveConnectionURL) {
        super(kuduClient, hiveConnectionURL);
    }


    @Override
    public void handleDeletion(FlowFile flowFile) throws Exception {

        String databaseName = flowFile.getAttribute("database_name");
        String tableName = flowFile.getAttribute("table_name");
        String[] new_values = flowFile.getAttribute("new_values").split(",");

        KuduTable table = kuduClient.openTable(tableName);
        Schema schema = table.getSchema();
        if ( tableName == "transactions") {
            int MT_CODE = Integer.parseInt(new_values[0]);

            //get Terminal_ID from Hive
            Integer merchantId;
            double transaction_amount;
            Connection conn = DriverManager.getConnection(hiveConnectionURL + "/" + databaseName, "hdfs", "");
            String query = "select * from transactions as a inner join terminals as b on a.TERM_ID = b.id inner join " +
                    " merchants as c on b.merch_id = c.id where MT_CODE = " + MT_CODE;
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query);
            rs.next();
            merchantId = rs.getInt("merch_id");
            String merchant_name = rs.getString("name");
            transaction_amount = rs.getDouble("TRAN_AMOUNT");
            int transactionCount = getTransactionCount(kuduClient, tableName, merchantId);
            double total_transaction_amount = getTransactionAmount(kuduClient, tableName, merchantId);
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
                    updateRow(kuduClient, merchantId, merchant_name, transaction_amount + total_transaction_amount, transactionCount + 1);
                }

        }else {
            // this means the delete is in merchants

            int merch_id = Integer.parseInt(new_values[0]);
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
        int MT_CODE = Integer.parseInt(new_values[0]);

        KuduTable table = kuduClient.openTable(tableName);
        Schema schema = table.getSchema();

        //get Terminal_ID from Hive
        Integer merchantId;
        double transaction_amount;
        Connection conn = DriverManager.getConnection(hiveConnectionURL + "/" + databaseName, "hdfs", "");
        String query = "select * from transactions as a inner join terminals as b on a.TERM_ID = b.id inner join " +
                " merchants as c on b.merch_id = c.id where MT_CODE = " + MT_CODE;
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(query);
        rs.next();
        merchantId = rs.getInt("merch_id");
        String merchant_name = rs.getString("name");
        transaction_amount = rs.getDouble("TRAN_AMOUNT");


        int transactionCount = getTransactionCount(kuduClient,tableName,merchantId);
        double total_transaction_amount = getTransactionAmount(kuduClient,tableName,merchantId);
        //Create new statement with inserting in kudu Number of transactions + 1
        if(transactionCount == 0){
            insertRow(kuduClient,merchantId,merchant_name,transaction_amount);
        }else{
            updateRow(kuduClient,merchantId,merchant_name ,transaction_amount+total_transaction_amount,transactionCount+1);
        }








    }




    private int getTransactionAmount(KuduClient client, String tableName, Integer merchant_id)  throws KuduException {

        KuduTable table = client.openTable(tableName);

        KuduScanner scanner = client.newScannerBuilder(table)
                .build();

        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                if(result.getInt(1) == merchant_id)
                    return result.getInt(3);
            }
        }
        return 0;
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
                    return result.getInt(4);}
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
        insert.getRow().addString("MERCH-NAME", merchant_name);
        insert.getRow().addInt("TRANSCOUNT", 1);
        insert.getRow().addDouble("TOTAL-AMT-TRANS", transaction_amount);
        session.apply(insert);
        session.close();
    }
    private void updateRow(KuduClient client,Integer merchantId,String merchant_name  ,double transaction_amount , Integer transaction_count) throws KuduException {

        KuduTable table = client.openTable(kuduTableName);
        KuduSession session = client.newSession();
        Update update = table.newUpdate();
        update.getRow().addString("ID",id);
        update.getRow().addInt("MERCH_ID", merchantId);
        update.getRow().addInt("TRANS-NUM", transaction_count);
        update.getRow().addString("MERCH-NAME", merchant_name);
        update.getRow().addDouble("TOTAL-AMT-TRANS", transaction_amount);
        session.apply(update);
        session.close();

    }


    @Override
    public void handleUpdate(FlowFile flowFile) throws Exception {

        String databaseName = flowFile.getAttribute("database_name");
        String tableName = flowFile.getAttribute("table_name");
        String[] new_values = flowFile.getAttribute("new_values").split(",");
        String[] old_values = flowFile.getAttribute("old_values").split(",");

        if ( tableName == "transactions"){

            int MT_CODE = Integer.parseInt(new_values[0]);

            KuduTable table = kuduClient.openTable(tableName);
            Schema schema = table.getSchema();

            //get Terminal_ID from Hive
            Integer merchantId;
            double transaction_amount;
            Connection conn = DriverManager.getConnection(hiveConnectionURL + "/" + databaseName, "hdfs", "");
            String query = "select * from transactions as a inner join terminals as b on a.TERM_ID = b.id inner join " +
                    " merchants as c on b.merch_id = c.id where MT_CODE = " + MT_CODE;
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query);
            rs.next();
            merchantId = rs.getInt("merch_id");
            String merchant_name = rs.getString("name");
            transaction_amount = Integer.parseInt(new_values[1]) - Integer.parseInt(old_values[1]);


            int transactionCount = getTransactionCount(kuduClient,tableName,merchantId);
            double total_transaction_amount = getTransactionAmount(kuduClient,tableName,merchantId);
            //Create new statement with inserting in kudu Number of transactions + 1

            updateRow(kuduClient,merchantId,merchant_name ,transaction_amount+total_transaction_amount,transactionCount);

        }else {
          // this mean the update happen in merchants

            int merch_id = Integer.parseInt(new_values[0]);

            String new_merch_name = new_values[1];
            String old_merch_name = old_values[1];
            if ( new_merch_name != old_merch_name){


                KuduTable table = kuduClient.openTable(kuduTableName);
                KuduSession session = kuduClient.newSession();
                Update update = table.newUpdate();
                update.getRow().addInt("MERCH_ID", merch_id);
                update.getRow().addString("MERCH-NAME", new_merch_name);
                session.apply(update);
                session.close();


            }

        }





    }
}
