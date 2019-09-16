package scc.processors.demo;

import org.apache.kudu.client.*;
import org.apache.nifi.flowfile.FlowFile;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class OffOnUs extends View {

    private final static String kuduTableName = "offonus";

    OffOnUs(KuduClient kuduClient, String hiveConnectionURL){
        super(kuduClient, hiveConnectionURL);
    }

    private static void deleteRow(KuduClient client, String id) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduSession session = client.newSession();
        Delete delete = table.newDelete();
        delete.getRow().addString("TIME", id);
        session.apply(delete);
    }

    private static void insertRow(KuduClient client, String recordDate, int[] transactionCounts) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduSession session = client.newSession();
        Insert insert = table.newInsert();
        insert.getRow().addString("TIME", recordDate);
        insert.getRow().addInt("OFF_US_COUNT", transactionCounts[0]);
        insert.getRow().addInt("ON_US_COUNT", transactionCounts[1]);
        session.apply(insert);
        session.close();
    }

    private static int[] getTransactionCount(KuduClient client, String recordDate) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduScanner scanner = client.newScannerBuilder(table)
                .build();
        int[] transactionCounts = {-1, -1};
        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                if(result.getString("TIME").equals(recordDate)){
                    transactionCounts[0] = result.getInt("OFF_US_COUNT");
                    transactionCounts[1] = result.getInt("ON_US_COUNT");
                    return transactionCounts;
                }
            }
        }
        return transactionCounts;
    }

    private static void updateRow(KuduClient client,String recordDate, int[] transactionCount) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduSession session = client.newSession();
        Update update = table.newUpdate();
        update.getRow().addString("TIME", recordDate);
        update.getRow().addInt("OFF_US_COUNT", transactionCount[0]);
        update.getRow().addInt("ON_US_COUNT", transactionCount[1]);
        session.apply(update);
    }

    private static String[] parseTimestamp(String timestamp) throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(timestamp);
        timestamp = timestamp.substring(1, timestamp.length() - 2);
        System.out.println(timestamp + " after");
        Date date = formatter.parse(timestamp);

        String[] time = new String[2];
        time[1] = Integer.toString(date.getYear() + 1900);
        time[0] = Integer.toString(date.getMonth() + 1);
        return time;
    }

    public void handleInsertion(FlowFile flowFile) throws Exception{
        String databaseName = flowFile.getAttribute("database_name");
        String tableName = flowFile.getAttribute("table_name");
        String keyValue = flowFile.getAttribute("primary_key");
        if(tableName.equals("transactions")){
            Connection conn = DriverManager.getConnection(hiveConnectionURL + "/" + databaseName, "hdfs", "");
            String query = "select * from transactions where mt_code = " + keyValue;
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query);
            rs.next();
            String recordDate = (rs.getTimestamp("record_date").getMonth() + 1)+ "-" + (rs.getTimestamp("record_date").getYear() + 1900);

            int[] transactionCounts = getTransactionCount(kuduClient, recordDate);

            if(transactionCounts[0] == -1){
                if(rs.getInt("tran_source") == rs.getInt("tran_dest")){
                    transactionCounts[0] = 0;
                    transactionCounts[1] = 1;
                    insertRow(kuduClient,recordDate,transactionCounts);
                }else{
                    transactionCounts[0] = 1;
                    transactionCounts[1] = 0;
                    insertRow(kuduClient,recordDate,transactionCounts);
                }
            }else{
                if(rs.getInt("tran_source") == rs.getInt("tran_dest")){
                    transactionCounts[1]++;
                    updateRow(kuduClient,recordDate,transactionCounts);
                }else{
                    transactionCounts[0]++;
                    updateRow(kuduClient,recordDate,transactionCounts);
                }
            }
        }
    }

    public void handleDeletion(FlowFile flowFile) throws Exception{
        String[] deletedValues = flowFile.getAttribute("new_values").split(",");
        String oldSource = deletedValues[4];
        String oldDest = deletedValues[5];
        String[] time = parseTimestamp(deletedValues[6]);
        String recordDate = time[0]+"-"+time[1];
        int[] transactionCount = getTransactionCount(kuduClient,recordDate);
        if(oldDest.equals(oldSource)){
            transactionCount[1]--;
        }else{
            transactionCount[0]--;
        }
        if(transactionCount[0] == transactionCount[1] && transactionCount[0] == 0){
            deleteRow(kuduClient, recordDate);
        }else{
            updateRow(kuduClient, recordDate, transactionCount);
        }
    }

    public void handleUpdate(FlowFile flowFile) throws Exception{
        String[] updatedValues = flowFile.getAttribute("new_values").split(",");
        String oldSource = updatedValues[8];
        String newSource = updatedValues[9];
        String oldDest = updatedValues[10];
        String newDest = updatedValues[11];
        String[] oldTime = parseTimestamp(updatedValues[12]);
        String[] newTime = parseTimestamp(updatedValues[13]);
        String oldRecordDate = oldTime[0]+"-"+oldTime[1];
        String newRecordDate = newTime[0]+"-"+newTime[1];
        int[] oldTransactionCount = getTransactionCount(kuduClient,oldRecordDate);
        int[] newTransactionCount = getTransactionCount(kuduClient,newRecordDate);
        if(oldDest.equals(oldSource)){
            oldTransactionCount[1]--;
        }else{
            oldTransactionCount[0]--;
        }

        if(oldTransactionCount[0] == oldTransactionCount[1] && oldTransactionCount[0] == 0){
            deleteRow(kuduClient, oldRecordDate);
        }else{
            updateRow(kuduClient, oldRecordDate, oldTransactionCount);
        }

        if(newTransactionCount[0] == -1){
            newTransactionCount[0] = 0;
            newTransactionCount[1] = 0;
            if(newDest.equals(newSource)){
                newTransactionCount[1]++;
            }else{
                newTransactionCount[0]++;
            }
            insertRow(kuduClient, newRecordDate,newTransactionCount);
        }else{
            if(newDest.equals(newSource)){
                newTransactionCount[1]++;
            }else{
                newTransactionCount[0]++;
            }
            updateRow(kuduClient, newRecordDate, newTransactionCount);
        }
    }
}
