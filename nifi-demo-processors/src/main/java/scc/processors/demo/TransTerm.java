package scc.processors.demo;


import org.apache.kudu.client.*;
import org.apache.nifi.flowfile.FlowFile;
import java.sql.*;
import java.util.Date;


public class TransTerm extends View {
    private final static String kuduTableName = "views::transaction-terminal";

    TransTerm(KuduClient kuduClient, String hiveConnectionURL){
        super(kuduClient, hiveConnectionURL);
    }

    private static int getTransactionCount(KuduClient client, int terminalId, String recordDate) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduScanner scanner = client.newScannerBuilder(table)
                .build();

        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                if(result.getInt("TERMID") == terminalId && result.getString("TIME").equals(recordDate))
                    return result.getInt("TRANSCOUNT");
            }
        }
        return 0;
    }

    private static String getRowId(KuduClient client, int terminalId, String recordDate) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduScanner scanner = client.newScannerBuilder(table)
                .build();

        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                if(result.getInt("TERMID") == terminalId && result.getString("TIME").equals(recordDate))
                    return result.getString("ID");
            }
        }
        return "0";
    }


    private static void updateRow(KuduClient client,String timestamp, int keyValue, int newTransCount, String rowId) throws KuduException {
        KuduTable table = client.openTable(kuduTableName);
        KuduSession session = client.newSession();
        Update update = table.newUpdate();
        update.getRow().addString("ID",rowId);
        update.getRow().addString("TIME",timestamp);
        update.getRow().addInt("TERMID", keyValue);
        update.getRow().addInt("TRANSCOUNT", newTransCount);
        session.apply(update);
    }

    private static void insertRow(KuduClient client, String timestamp, int keyValue, String rowId) throws KuduException {
        // Open the newly-created table and create a KuduSession.
        KuduTable table = client.openTable(kuduTableName);
        KuduSession session = client.newSession();
        Insert insert = table.newInsert();
        insert.getRow().addString("TIME", timestamp);
        insert.getRow().addInt("TERMID", keyValue);
        insert.getRow().addInt("TRANSCOUNT", 1);
        insert.getRow().addString("ID", rowId);
        session.apply(insert);
        session.close();
    }

    public void handleInsertion(FlowFile flowFile) throws Exception{
        String databaseName = flowFile.getAttribute("database_name");
        String tableName = flowFile.getAttribute("table_name");
        String keyValue = flowFile.getAttribute("primary_key");

        Date date = new Date();
        Long time = date.getTime();

        if(tableName == "transactions"){
            //set KuduTable
            KuduTable table = kuduClient.openTable(kuduTableName);

            //get Terminal_ID from Hive
            int terminalId;
            Connection conn = DriverManager.getConnection(hiveConnectionURL + "/" + databaseName, "hdfs", "");
            String query = "select * from transactions where MT_CODE = " + keyValue;
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query);
            rs.next();
            terminalId = rs.getInt("TERM_ID");
            String recordDate = (rs.getTimestamp("RECORD_DATE").getMonth() + 1)+ "-" + (rs.getTimestamp("RECORD_DATE").getYear() + 1900);



            //find entry of same terminal_id in Kudu and store Number of transactions
            int transactionCount = getTransactionCount(kuduClient,terminalId,recordDate);
            String rowId = getRowId(kuduClient,terminalId,recordDate);
            //Create new statement with inserting in kudu Number of transactions + 1
            if(transactionCount == 0){
                insertRow(kuduClient,recordDate,terminalId,String.valueOf(time));
            }else{
                updateRow(kuduClient,recordDate,terminalId,transactionCount+1,rowId);
            }

        }
    }

    private static String[] parseTimestamp(String timestamp){
        String[] time = new String[2];
        time[1] = timestamp.substring(0,4);
        time[0] = timestamp.substring(5,2);
        return time;
    }

    public void handleDeletion(FlowFile flowFile) throws KuduException{
        String[] deletedValues = flowFile.getAttribute("new_values").split(",");
        int terminalId = Integer.valueOf(deletedValues[2]);
        String[] time = parseTimestamp(deletedValues[6]);
        String recordDate = time[0]+"-"+time[1];
        int transactionCount = getTransactionCount(kuduClient,terminalId,recordDate);
        String rowId = getRowId(kuduClient,terminalId,recordDate);
        updateRow(kuduClient,recordDate,terminalId,transactionCount-1,rowId);
    }

    public void handleUpdate(FlowFile flowFile) throws KuduException{
        String[] updatedValues = flowFile.getAttribute("new_values").split(",");
        if(!updatedValues[4].equals(updatedValues[5])){
            int oldTerminalId = Integer.valueOf(updatedValues[4]);
            int newTerminalId = Integer.valueOf(updatedValues[5]);
            String[] oldTime = parseTimestamp(updatedValues[12]);
            String[] newTime = parseTimestamp(updatedValues[13]);
            String oldRecordDate = oldTime[0] + "-" + oldTime[1];
            String newRecordDate = newTime[0] + "-" + newTime[1];

            int transactionCount = getTransactionCount(kuduClient,oldTerminalId,oldRecordDate);
            String rowId = getRowId(kuduClient,oldTerminalId,oldRecordDate);

            if(transactionCount == 1){
                updateRow(kuduClient,oldRecordDate,oldTerminalId,transactionCount-1,rowId);
            }else{

            }

            transactionCount = getTransactionCount(kuduClient,newTerminalId,newRecordDate);
            rowId = getRowId(kuduClient,newTerminalId,newRecordDate);

            if(transactionCount == 0){
                insertRow(kuduClient,newRecordDate,newTerminalId,String.valueOf(time));
            }else{
                updateRow(kuduClient,newRecordDate,newTerminalId,transactionCount+1,rowId);
            }

        }
    }
}
