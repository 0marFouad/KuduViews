package scc.processors.demo;

import org.apache.kudu.client.*;
import org.apache.nifi.flowfile.FlowFile;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BankMerchant extends View {

    private final static String kuduTableName = "bankmerchant";

    BankMerchant(KuduClient kuduClient, String hiveConnectionURL){
        super(kuduClient, hiveConnectionURL);
    }

    private void insertRow(String trans_year, int bank_id, String bank_name, int merch_id, String merch_name, int trans_amt) throws KuduException {
        KuduTable table = kuduClient.openTable(kuduTableName);
        KuduScanner scanner = kuduClient.newScannerBuilder(table).build();
        KuduSession session = kuduClient.newSession();
        while (scanner.hasMoreRows()){
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()){
                RowResult result = results.next();
                if(result.getString("TIME").equals(trans_year)
                        && result.getInt("BANK_ID") == bank_id
                        && result.getInt("MERCH_ID") == merch_id)
                {
                    Update update = table.newUpdate();
                    update.getRow().addString("ID", result.getString("ID") );
                    update.getRow().addInt("TRANS_NUM", result.getInt("TRANS_NUM") + 1);
                    update.getRow().addInt("TRANS_AMT", result.getInt("TRANS_AMT") + trans_amt);
                    session.apply(update);
                    session.close();
                    return;
                }
            }
        }
        Insert insert = table.newInsert();
        Date date= new Date();
        Long time = date.getTime();

        insert.getRow().addString("ID", time.toString());
        insert.getRow().addString("TIME", trans_year);
        insert.getRow().addString("BANK_NAME", bank_name);
        insert.getRow().addString("MERCH_NAME", merch_name);
        insert.getRow().addInt("BANK_ID", bank_id);
        insert.getRow().addInt("MERCH_ID", merch_id);
        insert.getRow().addInt("TRANS_NUM", 1);
        insert.getRow().addInt("TRANS_AMT", trans_amt);
        session.apply(insert);
        session.close();
    }

    private void deleteRow(String trans_year, int bank_id, int merch_id, int trans_amt) throws Exception {
        KuduTable table = kuduClient.openTable(kuduTableName);
        KuduScanner scanner = kuduClient.newScannerBuilder(table).build();
        KuduSession session = kuduClient.newSession();
        while (scanner.hasMoreRows()){
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                if(result.getInt("BANK_ID") == bank_id && result.getString("TIME").equals(trans_year)
                        && result.getInt("MERCH_ID") == merch_id
                ){
                    if(result.getInt("TRANS_NUM") == 1){
                        Delete delete = table.newDelete();
                        delete.getRow().addString("ID", result.getString("ID"));
                        session.apply(delete);
                    } else {
                        Update update = table.newUpdate();
                        update.getRow().addString("ID", result.getString("ID") );
                        update.getRow().addInt("TRANS_NUM", result.getInt("TRANS_NUM") - 1);
                        update.getRow().addInt("TRANS_AMT", result.getInt("TRANS_AMT") - trans_amt);
                        session.apply(update);
                    }
                    session.close();
                }
            }
        }
    }

    @Override
    public void handleInsertion(FlowFile flowFile) throws Exception {
        String databaseName = flowFile.getAttribute("database_name");
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection(hiveConnectionURL + "/" + databaseName, "hdfs", "");
        Statement st = conn.createStatement();

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String tableName = flowFile.getAttribute("table_name");
        String[] new_values = flowFile.getAttribute("new_values").split(",");

        int bank_id = Integer.parseInt(new_values[4]);
        int term_id = Integer.parseInt(new_values[2]);
        int trans_amt = Integer.parseInt(new_values[1]);
        String trans_year = Integer.toString(formatter.parse(new_values[6].substring(1, new_values[6].length() - 1)).getYear() + 1900);

        //each row has unique (year + bankid + merchId)
        //to format each kudu row we need bankname, merch id and merch name so we hit hive
        if(tableName.toLowerCase().equals("transactions")){
            String query = "select * from banks where id = " + bank_id;
            ResultSet rs = st.executeQuery(query);
            rs.next();
            String bank_name = rs.getString("name");
            query = "select * from terminals where id = " + term_id;
            rs = st.executeQuery(query);
            rs.next();
            int merch_id = rs.getInt("merch_id");
            query = "select * from merchants where id = " + merch_id;
            rs = st.executeQuery(query);
            rs.next();
            String merch_name = rs.getString("name");
            insertRow(trans_year, bank_id, bank_name, merch_id, merch_name, trans_amt);
        }
    }

    @Override
    public void handleDeletion(FlowFile flowFile) throws Exception {
        String databaseName = flowFile.getAttribute("database_name");
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection(hiveConnectionURL + "/" + databaseName, "", "");
        Statement st = conn.createStatement();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String tableName = flowFile.getAttribute("table_name");
        String[] new_values = flowFile.getAttribute("new_values").split(",");

        int bank_id = Integer.parseInt(new_values[4]);
        int term_id = Integer.parseInt(new_values[2]);
        int trans_amt = Integer.parseInt(new_values[1]);
        String trans_year = Integer.toString(formatter.parse(new_values[6].substring(1,new_values[6].length() - 1)).getYear() + 1900);

        //each row has unique (year + bankid + merchId)
        //to format each kudu row we need bankname, merch id and merch name so we hit hive
        if(tableName.toLowerCase().equals("transactions")){
            String query = "select * from terminals where id = " + term_id;
            ResultSet rs = st.executeQuery(query);
            rs.next();
            int merch_id = rs.getInt("merch_id");
            deleteRow(trans_year, bank_id, merch_id, trans_amt);
        }
    }

    @Override
    public void handleUpdate(FlowFile flowFile) {

    }
}
