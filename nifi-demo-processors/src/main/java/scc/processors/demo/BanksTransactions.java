package scc.processors.demo;


import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.nifi.flowfile.FlowFile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class BanksTransactions extends View{



    private final static String kuduTableName = "merchant-profit";

    BanksTransactions(KuduClient kuduClient, String hiveConnectionURL) {
        super(kuduClient, hiveConnectionURL);
    }



    @Override
    public void handleInsertion(FlowFile flowFile) throws Exception {

        String databaseName = flowFile.getAttribute("database_name");
        String tableName = flowFile.getAttribute("table_name");
        String keyValue = flowFile.getAttribute("primary_key");
        if(tableName == "transactions"){
            //set KuduTable
            KuduTable table = kuduClient.openTable(tableName);
            Schema schema = table.getSchema();

            //get Terminal_ID from Hive
                Integer sourceBankId;
                Integer destBankId;
                Integer trans_count;
                Integer MT_CODE;
            double transaction_amount;
            Connection conn = DriverManager.getConnection(hiveConnectionURL + "/" + databaseName, "hdfs", "");
            String query = "select * from transactions where MT_CODE = " + keyValue;
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query);
            rs.next();
            sourceBankId = rs.getInt("tran_source");
            destBankId = rs.getInt("tran_dest");
            MT_CODE = rs.getInt("mt_code");
            transaction_amount = rs.getDouble("tran_amount");


            //find entry of same terminal_id in Kudu and store Number of transactions
            int transactionCount = getTransactionCount(kuduClient,tableName,MT_CODE);
            int total_transaction_amount = getTransactionAmount(kuduClient,tableName,MT_CODE);
            //Create new statement with inserting in kudu Number of transactions + 1
            if(transactionCount == 0){
                insertRow(kuduClient,MT_CODE,merchant_name,transaction_amount);
            }else{
                updateRow(kuduClient,merchantId,merchant_name ,transaction_amount,transactionCount+1);
            }

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
                if(result.getInt(0) == merchant_id)
                    return result.getInt(2);
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
                if(result.getInt(0) == merchant_id)
                    return result.getInt(3);
            }
        }
        return 0;
    }

    static void insertRow(KuduClient client, int mt_code,int sourceBank ,double transaction_amount,int destBank) throws KuduException {
        // Open the newly-created table and create a KuduSession.
        KuduTable table = client.openTable(kuduTableName);
        KuduSession session = client.newSession();
        Insert insert = table.newInsert();
        insert.getRow().addInt("MT-CODE", mt_code);
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
        update.getRow().addInt("MERCH_ID", merchantId);
        update.getRow().addInt("TRANS-NUM", transaction_count);
        update.getRow().addString("MERCH-NAME", merchant_name);
        update.getRow().addDouble("TOTAL-AMT-TRANS", transaction_amount);
        session.apply(update);
        session.close();

    }


    @Override
    public void handleDeletion() {

    }

    @Override
    public void handleUpdate() {

    }


}
