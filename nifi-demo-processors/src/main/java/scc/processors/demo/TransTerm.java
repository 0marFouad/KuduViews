package scc.processors.demo;

import java.util.List;
import java.util.Random;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.nifi.flowfile.FlowFile;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;

public class TransTerm extends View {


    TransTerm(KuduClient kuduClient, String hiveConnectionURL){
        super(kuduClient, hiveConnectionURL);
    }

    static int getTransactionCount(KuduClient client, String tableName, int terminalId) throws KuduException {
        KuduTable table = client.openTable(tableName);

        KuduScanner scanner = client.newScannerBuilder(table)
                .build();

        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                if(result.getInt(0) == terminalId)
                    return result.getInt(1);
            }
        }
        return 0;
    }

    static void updateRow(KuduClient client, String tableName, int keyValue, int newTransCount) throws KuduException {
        KuduTable table = client.openTable(tableName);
        KuduSession session = client.newSession();
        Update update = table.newUpdate();
        update.getRow().addInt("TERMID", keyValue);
        update.getRow().addInt("TRANSCOUNT", newTransCount);
        session.apply(update);
    }

    static void insertRow(KuduClient client, String tableName, int keyValue) throws KuduException {
        // Open the newly-created table and create a KuduSession.
        KuduTable table = client.openTable(tableName);
        KuduSession session = client.newSession();
        Insert insert = table.newInsert();
        insert.getRow().addInt("TERMID", keyValue);
        insert.getRow().addInt("TRANSCOUNT", 1);
        session.apply(insert);
        session.close();
    }

    public void handleInsertion(FlowFile flowFile) throws Exception{
        String databaseName = flowFile.getAttribute("database_name");
        String tableName = flowFile.getAttribute("table_name");
        String keyValue = flowFile.getAttribute("primary_key");
        if(tableName == "transactions"){
            //set KuduTable
            KuduTable table = kuduClient.openTable(tableName);
            Schema schema = table.getSchema();

            //get Terminal_ID from Hive
            Integer terminalId;
            Connection conn = DriverManager.getConnection(hiveConnectionURL + "/" + databaseName, "hdfs", "");
            String query = "select * from transactions where MT_CODE = " + keyValue;
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query);
            rs.next();
            terminalId = rs.getInt("TERM_ID");

            //find entry of same terminal_id in Kudu and store Number of transactions
            int transactionCount = getTransactionCount(kuduClient,tableName,terminalId);
            //Create new statement with inserting in kudu Number of transactions + 1
            if(transactionCount == 0){
                insertRow(kuduClient,tableName,terminalId);
            }else{
                updateRow(kuduClient,tableName,terminalId,transactionCount+1);
            }

        }
    }

    public void handleDeletion(){
        System.out.println("Deletion Goes Here");
    }

    public void handleUpdate(){
        System.out.println("Update Goes Here");
    }



//    public static void main(String[] args) throws IOException {
//        executeQuery();
//    }
//
//    private static void getDataFromHive(Operation operation, FlowFile flowFile, Connection hiveConnection) throws IOException {
//
//    }
//
//    private static void makeJson() {
//        String json = "{\"type\":\"insert\",\"timestamp\":1564847434000,\"binlog_filename\":\"test1.000053\",\"binlog_position\":352,\"database\":\"nifi_db\",\"table_name\":\"users_kudu\",\"table_id\":110,\"columns\":[{\"id\":1,\"name\":\"id\",\"column_type\":4,\"value\":19},{\"id\":2,\"name\":\"title\",\"column_type\":-1,\"value\":\"mr4\"}]}";
//        Object document = Configuration.defaultConfiguration().jsonProvider().parse(json);
//
//        String type = JsonPath.read(document, "$.type");
//        String currentTableName = JsonPath.read(document, "$.table_name");
//        System.out.println(currentTableName);
//
//        List<Integer> columnsTypes = JsonPath.read(document, "$.columns[*].column_type");
//        List<String> columnsNames = JsonPath.read(document, "$.columns[*].name");
//        List columnsValues = JsonPath.read(document, "$.columns[*].value");
//
//        System.out.println(columnsTypes);
//        System.out.println(columnsNames);
//        System.out.println(columnsValues);
//
//        for(int i = 0; i < columnsTypes.size(); i++) {
//            switch(columnsTypes.get(i)) {
//                case 4:
//                    System.out.println(columnsValues.get(i).toString());
//                    break;
//                case -1:
//                    System.out.println(columnsValues.get(i));
//                    break;
//            }
//        }
//    }
//
//    private static void executeQuery() {
//        try {
//            // create our mysql database connection
//            String myDriver = "com.mysql.cj.jdbc.Driver";
//            String myUrl = "jdbc:mysql://localhost:3306/banking3";
//            Class.forName(myDriver);
//            Connection conn = DriverManager.getConnection(myUrl, "root", "root");
//
//            // our SQL SELECT query.
//            // if you only need a few columns, specify them by name instead of using "*"
//            String query = "select MT_CODE, TRAN_AMOUNT, TERM_ID, CARD_ID, "
//                    + "TRAN_SOURCE, TRAN_DEST, RECORD_DATE, card_no, "
//                    + "a.name as custName, bSource.name as sourceName, "
//                    + "bDest.name as destName from transactions "
//                    + "as t inner join cards as c "
//                    + "on t.CARD_ID = c.id "
//                    + "inner join customers as a "
//                    + "on c.cust_id = a.id inner join banks as bSource "
//                    + "on bSource.id = t.TRAN_SOURCE "
//                    + "inner join banks as bDest on bDest.id = t.TRAN_DEST ";
////	      		+ "t.TRAN_AMOUNT = 200 and "
////	      		+ "t.TERM_ID = 300 and "
////	      		+ "t.CARD_ID = 400 and "
////	      		+ "t.TRAN_SOURCE = 500 and "
////	      		+ "t.TRAN_DEST = 700 and t.TRAN_DEST= '500';";
//
//            // create the java statement
//            Statement st = conn.createStatement();
//
//            // execute the query, and get a java resultset
//            ResultSet rs = st.executeQuery(query);
//
//            // iterate through the java resultset
//            while (rs.next())
//            {
//                int columnsCount = rs.getMetaData().getColumnCount();
//                for(int i = 1; i <= columnsCount; i++) {
//                    System.out.print(rs.getObject(i)+ ", ");
//                }
//                System.out.println();
//            }
////	      System.out.println(rs.getMetaData().getColumnType(7));
//            st.close();
//        } catch (Exception e) {
//            System.err.println("Got an exception! ");
//            System.err.println(e.getMessage());
//        }
//    }
}
