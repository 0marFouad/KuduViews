/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package scc.processors.demo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteSource;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import javax.security.auth.login.LoginException;
import java.sql.*;

import org.apache.commons.io.Charsets;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.Upsert;
import org.apache.kudu.client.KuduClient.KuduClientBuilder;
import org.apache.kudu.client.SessionConfiguration.FlushMode;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosKeytabUser;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.serialization.record.Record;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Configuration;


@EventDriven
@SupportsBatching
@RequiresInstanceClassLoading
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"put", "database", "NoSQL", "kudu", "HDFS", "record"})
@CapabilityDescription("Reads records from an incoming FlowFile using the provided Record Reader, and writes those records to the specified Kudu's table. The schema for the table must be provided in the processor properties or from your source. If any error occurs while reading records from the input, or writing records to Kudu, the FlowFile will be routed to failure")
@WritesAttribute(
    attribute = "record.count",
    description = "Number of records written to Kudu"
)
public class PutMaterializedToKudu extends AbstractProcessor {
    protected static final PropertyDescriptor KUDU_MASTERS;
    static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE;
//    protected static final PropertyDescriptor SKIP_HEAD_LINE;
    protected static final PropertyDescriptor FLUSH_MODE;
    protected static final PropertyDescriptor QUERIES;
    protected static final PropertyDescriptor HIVE_URL;
//    protected static final PropertyDescriptor FLOWFILE_BATCH_SIZE;
//    protected static final PropertyDescriptor BATCH_SIZE;
    protected static final Relationship REL_SUCCESS;
    protected static final Relationship REL_FAILURE;
//    public static final String RECORD_COUNT_ATTR = "record.count";
    protected FlushMode flushMode;
    protected int batchSize = 100;
    protected int ffbatch = 1;
    protected KuduClient kuduClient;
    protected KuduTable kuduTable;
    protected String queriesJson;
    protected String hiveConnectionURL;
    private volatile KerberosUser kerberosUser;
    private Map<String,ArrayList<View>> tablesEditor = new HashMap< String,ArrayList<View>>();
    private String tableName = null;
    private Connection conn = null;
    private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

    public PutMaterializedToKudu() {
        View TransTerm = new TransTerm(kuduClient,hiveConnectionURL);
        View OffOnUs = new OffOnUs(kuduClient,hiveConnectionURL);
        View merchantProfit = new merchantProfit(kuduClient, hiveConnectionURL);
        View BankCustomer = new BankCustomer(kuduClient, hiveConnectionURL);
        View BankMerchant = new BankMerchant(kuduClient, hiveConnectionURL);
        View BanksTransactions = new BanksTransactions(kuduClient, hiveConnectionURL);
        View Customer_Merch = new Customer_Merch(kuduClient, hiveConnectionURL);
        View Customer_Report = new Customer_Report(kuduClient, hiveConnectionURL);

        ArrayList<View> tranList = new ArrayList<>();
        tranList.add(merchantProfit);
        tranList.add(BankMerchant);
        tranList.add(BanksTransactions);
        tranList.add(Customer_Merch);
        tranList.add(Customer_Report);
        tranList.add(TransTerm);
        tranList.add(OffOnUs);
        tablesEditor.put("transactions",tranList);

        ArrayList<View> custList = new ArrayList<>();
        custList.add(Customer_Report);
        custList.add(Customer_Merch);
        custList.add(BankCustomer);
        tablesEditor.put("customers",custList);

        ArrayList<View> mercList = new ArrayList<>();
        mercList.add(Customer_Merch);
        mercList.add(merchantProfit);
        mercList.add(BankMerchant);
        tablesEditor.put("merchants",mercList);
        tablesEditor.put("banks", new ArrayList<>());
        tablesEditor.put("cards", new ArrayList<>());
        tablesEditor.put("terminals", new ArrayList<>());

    }

    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> properties = new ArrayList();
        properties.add(KUDU_MASTERS);
        properties.add(QUERIES);
        properties.add(HIVE_URL);
        properties.add(KERBEROS_CREDENTIALS_SERVICE);
        properties.add(FLUSH_MODE);
        return properties;
    }

    public Set<Relationship> getRelationships() {
        Set<Relationship> rels = new HashSet();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return rels;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) throws IOException, LoginException {
        String kuduMasters = context.getProperty(KUDU_MASTERS).evaluateAttributeExpressions().getValue();
        this.queriesJson = context.getProperty(QUERIES).getValue();
        this.hiveConnectionURL = context.getProperty(HIVE_URL).getValue();
        this.flushMode = FlushMode.valueOf(context.getProperty(FLUSH_MODE).getValue());
        this.getLogger().debug("Setting up Kudu connection...");
        KerberosCredentialsService credentialsService = (KerberosCredentialsService)context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);
        this.kuduClient = this.createClient(kuduMasters, credentialsService);
    }

    protected KuduClient createClient(String masters, KerberosCredentialsService credentialsService) throws LoginException {
        if (credentialsService == null) {
            return this.buildClient(masters);
        } else {
            String keytab = credentialsService.getKeytab();
            String principal = credentialsService.getPrincipal();
            this.kerberosUser = this.loginKerberosUser(principal, keytab);
            KerberosAction<KuduClient> kerberosAction = new KerberosAction(this.kerberosUser, () -> {
                return this.buildClient(masters);
            }, this.getLogger());
            return (KuduClient)kerberosAction.execute();
        }
    }

    protected KuduClient buildClient(String masters) {
        return (new KuduClientBuilder(masters)).build();
    }

    protected KerberosUser loginKerberosUser(String principal, String keytab) throws LoginException {
        KerberosUser kerberosUser = new KerberosKeytabUser(principal, keytab);
        kerberosUser.login();
        return kerberosUser;
    }

    @OnStopped
    public final void closeClient() throws KuduException, LoginException {
        try {
            if (this.kuduClient != null) {
                this.getLogger().debug("Closing KuduClient");
                this.kuduClient.close();
                this.kuduClient = null;
            }
        } finally {
            if (this.kerberosUser != null) {
                this.kerberosUser.logout();
                this.kerberosUser = null;
            }
        }
    }

    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        List<FlowFile> flowFiles = session.get(this.ffbatch);
        if (!flowFiles.isEmpty()) {
            KerberosUser user = this.kerberosUser;
            if (user == null) {
                this.trigger(context, session, flowFiles);
            } else {
                PrivilegedExceptionAction<Void> privelegedAction = () -> {
                    this.trigger(context, session, flowFiles);
                    return null;
                };
                KerberosAction<Void> action = new KerberosAction(user, privelegedAction, this.getLogger());
                action.execute();
            }
        }
    }

    private void trigger(ProcessContext context, ProcessSession session, List<FlowFile> flowFiles) throws ProcessException {
    	FlowFile flowFile = flowFiles.get(0);
		try {
            String tableName = flowFile.getAttribute("table_name");
            TransTerm tt = new TransTerm(kuduClient,hiveConnectionURL);
            OffOnUs offon = new OffOnUs(kuduClient,hiveConnectionURL);
            merchantProfit mp = new merchantProfit(kuduClient, hiveConnectionURL);
            BankCustomer bc = new BankCustomer(kuduClient, hiveConnectionURL);
            BankMerchant bm = new BankMerchant(kuduClient, hiveConnectionURL);
            BanksTransactions bt = new BanksTransactions(kuduClient, hiveConnectionURL);
            Customer_Merch cm = new Customer_Merch(kuduClient, hiveConnectionURL);
            Customer_Report cr = new Customer_Report(kuduClient, hiveConnectionURL);

            if(tableName.equals("transactions")){
                this.getLogger().debug(tableName);
                this.getLogger().debug(flowFile.getAttribute("query_type"));

                tt.execute(flowFile.getAttribute("query_type"),flowFile);
                offon.execute(flowFile.getAttribute("query_type"),flowFile);
                mp.execute(flowFile.getAttribute("query_type"),flowFile);
              /*   bm.execute(flowFile.getAttribute("query_type"),flowFile);
                bt.execute(flowFile.getAttribute("query_type"),flowFile);
                cm.execute(flowFile.getAttribute("query_type"),flowFile);
                cr.execute(flowFile.getAttribute("query_type"),flowFile);*/
            }


            if(tableName.equals("customers")){
                cm.execute(flowFile.getAttribute("query_type"),flowFile);
                cr.execute(flowFile.getAttribute("query_type"),flowFile);
            }
            if(tableName.equals("cards")){
                bc.execute(flowFile.getAttribute("query_type"),flowFile);
            }

            if(tableName.equals("merchants")){
                cm.execute(flowFile.getAttribute("query_type"),flowFile);
                mp.execute(flowFile.getAttribute("query_type"),flowFile);
                bm.execute(flowFile.getAttribute("query_type"),flowFile);
            }

            this.getLogger().debug("Managed to Insert");
	    	session.transfer(flowFile, REL_SUCCESS);
		} catch (Exception e) {
			e.printStackTrace();
			this.getLogger().error(e.getMessage());
			this.getLogger().debug("Failed to Insert");
    	    session.transfer(flowFile, REL_FAILURE);
    	    return;
		}
    }



    protected KuduSession getKuduSession(KuduClient client) {
        KuduSession kuduSession = client.newSession();
        kuduSession.setMutationBufferSpace(this.batchSize);
        kuduSession.setFlushMode(this.flushMode);

        return kuduSession;
    }

    private void flushKuduSession(KuduSession kuduSession, boolean close, List<RowError> rowErrors) throws KuduException {
        List<OperationResponse> responses = close ? kuduSession.close() : kuduSession.flush();
        if (kuduSession.getFlushMode() == FlushMode.AUTO_FLUSH_BACKGROUND) {
            rowErrors.addAll(Arrays.asList(kuduSession.getPendingErrors().getRowErrors()));
        } else {
            responses.stream().filter(OperationResponse::hasRowError).map(OperationResponse::getRowError).forEach(rowErrors::add);
        }
    }

    protected Upsert upsertRecordToKudu(KuduTable kuduTable, Record record, List<String> fieldNames) {
        Upsert upsert = kuduTable.newUpsert();
        this.buildPartialRow(kuduTable.getSchema(), upsert.getRow(), record, fieldNames);
        return upsert;
    }

    protected Insert insertRecordToKudu(KuduTable kuduTable, Record record, List<String> fieldNames) {
        Insert insert = kuduTable.newInsert();
        this.buildPartialRow(kuduTable.getSchema(), insert.getRow(), record, fieldNames);
        return insert;
    }

    @VisibleForTesting
    void buildPartialRow(Schema schema, PartialRow row, Record record, List<String> fieldNames) {
        Iterator var5 = fieldNames.iterator();

        while(var5.hasNext()) {
            String colName = (String)var5.next();
            int colIdx = this.getColumnIndex(schema, colName);
            if (colIdx != -1) {
                ColumnSchema colSchema = schema.getColumnByIndex(colIdx);
                Type colType = colSchema.getType();
                if (record.getValue(colName) == null) {
                    row.setNull(colName);
                } else {
                    switch(colType.getDataType(colSchema.getTypeAttributes())) {
                    case BOOL:
                        row.addBoolean(colIdx, record.getAsBoolean(colName));
                        break;
                    case FLOAT:
                        row.addFloat(colIdx, record.getAsFloat(colName));
                        break;
                    case DOUBLE:
                        row.addDouble(colIdx, record.getAsDouble(colName));
                        break;
                    case BINARY:
                        row.addBinary(colIdx, record.getAsString(colName).getBytes());
                        break;
                    case INT8:
                        row.addByte(colIdx, record.getAsInt(colName).byteValue());
                        break;
                    case INT16:
                        row.addShort(colIdx, record.getAsInt(colName).shortValue());
                        break;
                    case INT32:
                        row.addInt(colIdx, record.getAsInt(colName));
                        break;
                    case INT64:
                    case UNIXTIME_MICROS:
                        row.addLong(colIdx, record.getAsLong(colName));
                        break;
                    case STRING:
                        row.addString(colIdx, record.getAsString(colName));
                        break;
                    case DECIMAL32:
                    case DECIMAL64:
                    case DECIMAL128:
                        row.addDecimal(colIdx, new BigDecimal(record.getAsString(colName)));
                        break;
                    default:
                        throw new IllegalStateException(String.format("unknown column type %s", colType));
                    }
                }
            }
        }
    }

    private int getColumnIndex(Schema columns, String colName) {
        try {
            return columns.getColumnIndex(colName);
        } catch (Exception var4) {
            return -1;
        }
    }

    static {
        KUDU_MASTERS = (new Builder()).name("Kudu Masters").description("List all kudu masters's ip with port (e.g. 7051), comma separated").required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();
        KERBEROS_CREDENTIALS_SERVICE = (new Builder()).name("kerberos-credentials-service").displayName("Kerberos Credentials Service").description("Specifies the Kerberos Credentials to use for authentication").required(false).identifiesControllerService(KerberosCredentialsService.class).build();
//        SKIP_HEAD_LINE = (new Builder()).name("Skip head line").description("Deprecated. Used to ignore header lines, but this should be handled by a RecordReader (e.g. \"Treat First Line as Header\" property of CSVReader)").allowableValues(new String[]{"true", "false"}).defaultValue("false").required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
        FLUSH_MODE = (new Builder()).name("Flush Mode").description("Set the new flush mode for a kudu session.\nAUTO_FLUSH_SYNC: the call returns when the operation is persisted, else it throws an exception.\nAUTO_FLUSH_BACKGROUND: the call returns when the operation has been added to the buffer. This call should normally perform only fast in-memory operations but it may have to wait when the buffer is full and there's another buffer being flushed.\nMANUAL_FLUSH: the call returns when the operation has been added to the buffer, else it throws a KuduException if the buffer is full.").allowableValues(FlushMode.values()).defaultValue(FlushMode.AUTO_FLUSH_BACKGROUND.toString()).required(true).build();
//        FLOWFILE_BATCH_SIZE = (new Builder()).name("FlowFiles per Batch").description("The maximum number of FlowFiles to process in a single execution, between 1 - 100000. Depending on your memory size, and data size per row set an appropriate batch size for the number of FlowFiles to process per client connection setup.Gradually increase this number, only if your FlowFiles typically contain a few records.").defaultValue("1").required(true).addValidator(StandardValidators.createLongValidator(1L, 100000L, true)).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();
//        BATCH_SIZE = (new Builder()).name("Batch Size").displayName("Max Records per Batch").description("The maximum number of Records to process in a single Kudu-client batch, between 1 - 100000. Depending on your memory size, and data size per row set an appropriate batch size. Gradually increase this number to find out the best one for best performances.").defaultValue("100").required(true).addValidator(StandardValidators.createLongValidator(1L, 100000L, true)).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();
        QUERIES = (new Builder()).name("Queries").displayName("Queries from hive").description("").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
        HIVE_URL = (new Builder()).name("Hive Connection URL").displayName("Hive Connection URL").description("").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(true).build();
        REL_SUCCESS = (new org.apache.nifi.processor.Relationship.Builder()).name("success").description("A FlowFile is routed to this relationship after it has been successfully stored in Kudu").build();
        REL_FAILURE = (new org.apache.nifi.processor.Relationship.Builder()).name("failure").description("A FlowFile is routed to this relationship if it cannot be sent to Kudu").build();
    }
}
