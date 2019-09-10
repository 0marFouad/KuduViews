package scc.processors.demo;

import org.apache.kudu.client.*;
import org.apache.nifi.flowfile.FlowFile;
import java.text.SimpleDateFormat;
import java.util.Date;

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
                    update.getRow().addString("ID", result.getString("ID"));
                    update.getRow().addInt("CUSTOMERS_NUM", result.getInt("CUSTOMERS_NUM") + 1);
                    session.apply(update);
                    session.close();
                } else{
                    Insert insert = table.newInsert();
                    insert.getRow().addString("TIME", reg_date);
                    insert.getRow().addInt("BANK_ID", bank_id);

                    Date date= new Date();
                    Long time = date.getTime();

                    insert.getRow().addString("ID", time.toString());
                    insert.getRow().addInt("CUSTOMERS_NUM", 1);
                    session.apply(insert);
                    session.close();
                }

            }
        }

    }
    private void deleteRow(int bank_id, String reg_date) throws KuduException {
        KuduTable table = kuduClient.openTable(kuduTableName);
        KuduScanner scanner = kuduClient.newScannerBuilder(table).build();
        KuduSession session = kuduClient.newSession();
        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                if(result.getInt("BANK_ID") == bank_id && result.getString("REG_DATE").equals(reg_date)){
                    Delete delete = table.newDelete();
                    delete.getRow().addString("ID", result.getString("ID"));
                    session.apply(delete);
                    session.close();
                }
            }
        }
    }

    @Override
    public void handleInsertion(FlowFile flowFile) throws Exception {
        String tableName = flowFile.getAttribute("table_name");
        String[] new_values = flowFile.getAttribute("new_values").split(",");
        int bank_id = Integer.parseInt(new_values[3]);
        String reg_date = new_values[1];
        SimpleDateFormat formatter =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentYear = Integer.toString(formatter.parse(reg_date).getYear());
        if(tableName.toLowerCase().equals("cards")){
            insertRow(bank_id, currentYear);
        }
    }

    @Override
    public void handleDeletion(FlowFile flowFile) throws Exception {
        String tableName = flowFile.getAttribute("table_name");
        String[] values = flowFile.getAttribute("new_values").split(",");
        int bank_id = Integer.parseInt(values[3]);
        String reg_date = values[1];
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentYear = Integer.toString(formatter.parse(reg_date).getYear());
        if(tableName.toLowerCase().equals("cards")){
            deleteRow(bank_id, currentYear);
        }
    }



    @Override
    public void handleUpdate(FlowFile flowFile) {

    }
}
