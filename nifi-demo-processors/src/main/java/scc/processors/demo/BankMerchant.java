package scc.processors.demo;

import org.apache.kudu.client.KuduClient;
import org.apache.nifi.flowfile.FlowFile;

public class BankMerchant extends View {

    BankMerchant(KuduClient kuduClient, String hiveConnectionURL){
        super(kuduClient, hiveConnectionURL);
    }

    @Override
    public void handleInsertion(FlowFile flowFile) throws Exception {

    }

    @Override
    public void handleDeletion() {

    }

    @Override
    public void handleUpdate() {

    }
}
