package scc.processors.demo;

import org.apache.kudu.client.KuduClient;
import org.apache.nifi.flowfile.FlowFile;

abstract class View{
    protected final KuduClient kuduClient;
    protected final String hiveConnectionURL;
    protected final String hiveDriver = "org.apache.hive.jdbc.HiveDriver";

    View(KuduClient kuduClient, String hiveConnectionURL){
        this.kuduClient = kuduClient;
        this.hiveConnectionURL = hiveConnectionURL;
    }


    public abstract void handleInsertion(FlowFile flowFile) throws Exception;

    public abstract void handleDeletion(FlowFile flowFile) throws Exception;

    public abstract void handleUpdate(FlowFile flowFile) throws Exception;

    public final  void execute(String type,FlowFile flowFile) throws Exception{
        if(type.equals("insert")){
            handleInsertion(flowFile);
        }
        if(type.equals("delete")){
            handleDeletion(flowFile);
        }
        if(type.equals("update")){
            handleUpdate(flowFile);
        }
    }
}