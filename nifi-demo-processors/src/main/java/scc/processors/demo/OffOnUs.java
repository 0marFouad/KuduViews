package scc.processors.demo;


import org.apache.kudu.client.*;
import org.apache.nifi.flowfile.FlowFile;
import java.sql.*;

public class OffOnUs extends View {

    private final static String kuduTableName = "views::off-on-us";

    OffOnUs(KuduClient kuduClient, String hiveConnectionURL){
        super(kuduClient, hiveConnectionURL);
    }

//    static int getTransactionCount(KuduClient client, int terminalId, String recordDate) throws KuduException {
//        KuduTable table = client.openTable(kuduTableName);
//        KuduScanner scanner = client.newScannerBuilder(table)
//                .build();
//
//        while (scanner.hasMoreRows()) {
//            RowResultIterator results = scanner.nextRows();
//            while (results.hasNext()) {
//                RowResult result = results.next();
//                if(result.getInt(1) == terminalId && result.getString(0).equals(recordDate))
//                    return result.getInt(2);
//            }
//        }
//        return 0;
//    }

//    static void updateRow(KuduClient client,String timestamp, int keyValue, int newTransCount) throws KuduException {
//        KuduTable table = client.openTable(kuduTableName);
//        KuduSession session = client.newSession();
//        Update update = table.newUpdate();
//        update.getRow().addString("TIME",timestamp);
//        update.getRow().addInt("TERMID", keyValue);
//        update.getRow().addInt("TRANSCOUNT", newTransCount);
//        session.apply(update);
//    }

//    static void insertRow(KuduClient client, String timestamp, int keyValue) throws KuduException {
//        // Open the newly-created table and create a KuduSession.
//        KuduTable table = client.openTable(kuduTableName);
//        KuduSession session = client.newSession();
//        Insert insert = table.newInsert();
//        insert.getRow().addString("TIME", timestamp);
//        insert.getRow().addInt("TERMID", keyValue);
//        insert.getRow().addInt("TRANSCOUNT", 1);
//        session.apply(insert);
//        session.close();
//    }

    public void handleInsertion(FlowFile flowFile) throws Exception{
        String databaseName = flowFile.getAttribute("database_name");
        String tableName = flowFile.getAttribute("table_name");
        String keyValue = flowFile.getAttribute("primary_key");
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
            //Create new statement with inserting in kudu Number of transactions + 1
            if(transactionCount == 0){
                insertRow(kuduClient,recordDate,terminalId);
            }else{
                updateRow(kuduClient,recordDate,terminalId,transactionCount+1);
            }

        }
    }

    public void handleDeletion(){
        System.out.println("Deletion Goes Here");
    }

    public void handleUpdate(){
        System.out.println("Update Goes Here");
    }
}
