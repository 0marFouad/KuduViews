package scc.processors.demo;

import org.apache.kudu.client.*;
import org.apache.nifi.flowfile.FlowFile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class BankCustomer extends View {

    private final static String kuduTableName = "views::bank-customer";

    BankCustomer(KuduClient kuduClient, String hiveConnectionURL){
        super(kuduClient, hiveConnectionURL);
    }

    private void insertRow(int bank_id, String reg_date) throws KuduException {
        //set KuduTable
        KuduTable table = kuduClient.openTable(kuduTableName);
        KuduScanner scanner = kuduClient.newScannerBuilder(table).build();
        KuduSession session = kuduClient.newSession();
        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                if(result.getInt("BANK_ID") == bank_id && result.getString("REG_DATE").equals(reg_date)){
                    Update update = table.newUpdate();
                    update.getRow().addString("TIME",reg_date);
                    update.getRow().addInt("BANK_ID", bank_id);
                    update.getRow().addInt("ID", result.getInt("ID"));
                    update.getRow().addInt("CUSTOMERS_NUM", result.getInt("CUSTOMERS_NUM") + 1);
                    session.apply(update);
                    session.close();
                } else{
                    Insert insert = table.newInsert();
                    insert.getRow().addString("TIME", reg_date);
                    insert.getRow().addInt("BANK_ID", bank_id);
                    insert.getRow().addInt("ID", bank_id);
                    insert.getRow().addInt("CUSTOMERS_NUM", 1);
                    session.apply(insert);
                    session.close();
                }

            }
        }

    }
    private void deleteRow(int bank_id, String reg_date) {

    }

    @Override
    public void handleInsertion(FlowFile flowFile) throws Exception {
//        String databaseName = flowFile.getAttribute("database_name");
        String tableName = flowFile.getAttribute("table_name");
//        String keyValue = flowFile.getAttribute("primary_key");
        String[] new_values = flowFile.getAttribute("new_values").split(",");
        int bank_id = Integer.parseInt(new_values[1]);
        String reg_date = new_values[2];
        if(tableName.toLowerCase().equals("cards")){
            insertRow(bank_id, reg_date);
        }
    }

    @Override
    public void handleDeletion(FlowFile flowFile) {
        String tableName = flowFile.getAttribute("table_name");
//        String keyValue = flowFile.getAttribute("primary_key");
        String[] new_values = flowFile.getAttribute("new_values").split(",");
        int bank_id = Integer.parseInt(new_values[1]);
        String reg_date = new_values[2];
        if(tableName.toLowerCase().equals("cards")){
            deleteRow(bank_id, reg_date);
        }
    }



    @Override
    public void handleUpdate(FlowFile flowFile) {

    }
}
