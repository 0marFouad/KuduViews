package scc.processors.demo;


import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.nifi.flowfile.FlowFile;

import java.util.Date;

public class BanksTransactions extends View{



    private final static String kuduTableName = "views::bank-transaction";

    private  int total_transaction_amount =0  ;
    private String ID ;
    BanksTransactions(KuduClient kuduClient, String hiveConnectionURL) {
        super(kuduClient, hiveConnectionURL);
    }



    @Override
    public void handleInsertion(FlowFile flowFile) throws Exception {

        String databaseName = flowFile.getAttribute("database_name");
        String tableName = flowFile.getAttribute("table_name");
        if(tableName == "transactions"){
            //set KuduTable
            KuduTable table = kuduClient.openTable(tableName);
            Schema schema = table.getSchema();

            //get Terminal_ID from Hive

                Integer MT_CODE;
            double transaction_amount;
            String[] values = flowFile.getAttribute("new_values").split(",");
            Integer sourceBankId=Integer.parseInt(values[4]);;
            Integer destBankId=Integer.parseInt(values[5]);;
            Integer trans_amount=Integer.parseInt(values[1]);;
            //find entry of same terminal_id in Kudu and store Number of transactions
            int transactionCount = getTransactionCount(kuduClient,tableName,sourceBankId,destBankId);
            //Create new statement with inserting in kudu Number of transactions + 1
            if(transactionCount == 0){
                insertRow(kuduClient,sourceBankId,destBankId,transactionCount,trans_amount+total_transaction_amount);
            }else{
                updateRow(kuduClient,sourceBankId,destBankId,transactionCount,trans_amount+total_transaction_amount);
            }

        }
    }

    private void updateRow(KuduClient kuduClient, Integer sourceBankId, Integer destBankId, int transactionCount, int i) throws Exception {

        KuduTable table = kuduClient.openTable(kuduTableName);
        KuduSession session = kuduClient.newSession();
        Update update = table.newUpdate();
        update.getRow().addString("ID", ID);
        update.getRow().addInt("FROMBANK", sourceBankId);
        update.getRow().addInt("TOBANK", destBankId);
        update.getRow().addInt("TRANS-NUM", transactionCount);
        update.getRow().addDouble("TOTAL-AMT-TRANS", i);
        session.apply(update);
        session.close();

    }

    private int getTransactionCount(KuduClient kuduClient, String tableName, Integer sourceBankId, Integer destBankId) throws Exception {


        KuduTable table = kuduClient.openTable(tableName);

        KuduScanner scanner = kuduClient.newScannerBuilder(table)
                .build();

        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                if(result.getInt(0) == sourceBankId && result.getInt(1)==destBankId ){
                    total_transaction_amount = result.getInt(3);
                    ID = result.getString(0);
                    return result.getInt(2);
                }
            }
        }
        return 0;
    }

    private void insertRow(KuduClient kuduClient, Integer sourceBankId, Integer destBankId, int transactionCount, Integer trans_amount) throws Exception {
        KuduTable table = kuduClient.openTable(kuduTableName);
        KuduSession session = kuduClient.newSession();

        Date date= new Date();

        Long time = date.getTime();

        Insert insert = table.newInsert();

        insert.getRow().addString("ID",String.valueOf(time));
        insert.getRow().addInt("FROMBANK", sourceBankId);
        insert.getRow().addInt("TOBANK", destBankId);
        insert.getRow().addInt("TRANS-NUM", transactionCount);
        insert.getRow().addDouble("TOTAL-AMT-TRANS", trans_amount);
        session.apply(insert);
        session.close();


    }

    @Override
    public void handleDeletion(FlowFile flowFile) throws Exception {

        String databaseName = flowFile.getAttribute("database_name");
        String tableName = flowFile.getAttribute("table_name");
        if(tableName == "transactions"){

            //get Terminal_ID from Hive
            Integer MT_CODE;
            double transaction_amount;
            String[] values = flowFile.getAttribute("new_values").split(",");
            Integer sourceBankId=Integer.parseInt(values[4]);
            Integer destBankId=Integer.parseInt(values[5]);
            int transaction_count=getTransactionCount(kuduClient,tableName,sourceBankId,destBankId);
            KuduTable table = kuduClient.openTable(tableName);
            if(transaction_count!=0){

                KuduSession session = kuduClient.newSession();
                Delete delete = table.newDelete();
                delete.getRow().addString("ID", ID);
                session.apply(delete);
                session.close();
            }
        } else {

            // handle if deletion happened in banks
        }

    }

    @Override
    public void handleUpdate(FlowFile flowFile) throws Exception {


        String databaseName = flowFile.getAttribute("database_name");
        String tableName = flowFile.getAttribute("table_name");
        String[] new_values = flowFile.getAttribute("new_values").split(",");
        String[] old_values = flowFile.getAttribute("old_values").split(",");

        if ( tableName == "transactions"){

            int old_trans_amount = Integer.parseInt(old_values[3]);
            int new_trans_amount = Integer.parseInt(new_values[3]);
            int transactionCount = Integer.parseInt(new_values[2]);
            Integer sourceBankId=Integer.parseInt(new_values[4]);;
            Integer destBankId=Integer.parseInt(new_values[5]);;
            getTransactionCount(kuduClient,tableName,sourceBankId,destBankId);
            if(old_trans_amount != new_trans_amount){
                updateRow(kuduClient,sourceBankId,destBankId,transactionCount,new_trans_amount-old_trans_amount+total_transaction_amount);

            }
        }


    }








}
