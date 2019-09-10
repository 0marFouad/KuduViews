package scc.processors.demo;

import org.apache.kudu.client.KuduClient;
import org.apache.nifi.flowfile.FlowFile;

public class BankMerchant extends View {

    private final static String kuduTableName = "views::bank-merchant";

    BankMerchant(KuduClient kuduClient, String hiveConnectionURL){
        super(kuduClient, hiveConnectionURL);
    }

    @Override
    public void handleInsertion(FlowFile flowFile) throws Exception {
        String databaseName = flowFile.getAttribute("database_name");
        String tableName = flowFile.getAttribute("table_name");
        String keyValue = flowFile.getAttribute("primary_key");
    }

    @Override
    public void handleDeletion(FlowFile flowFile) {

    }

    @Override
    public void handleUpdate(FlowFile flowFile) {

    }
}
