/*
 * Copyright 2016-2017 ForgeRock AS. All Rights Reserved
 *
 * Use of this code requires a commercial software license with ForgeRock AS.
 * or with one of its affiliates. All use shall be exclusively subject
 * to such license between the licensee and ForgeRock AS.
 */

package org.forgerock.openicf.connectors.kafka;

import java.net.UnknownHostException;
import java.util.Locale;
import java.util.Set;

import org.identityconnectors.common.Assertions;
import org.identityconnectors.common.CollectionUtil;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.script.ScriptExecutor;
import org.identityconnectors.common.script.ScriptExecutorFactory;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.common.security.SecurityUtil;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.AttributeInfoBuilder;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.AttributesAccessor;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.ObjectClassInfoBuilder;
import org.identityconnectors.framework.common.objects.OperationOptionInfoBuilder;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.OperationalAttributeInfos;
import org.identityconnectors.framework.common.objects.PredefinedAttributeInfos;
import org.identityconnectors.framework.common.objects.ResultsHandler;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SchemaBuilder;
import org.identityconnectors.framework.common.objects.ScriptContext;
import org.identityconnectors.framework.common.objects.SearchResult;
import org.identityconnectors.framework.common.objects.SyncDelta;
import org.identityconnectors.framework.common.objects.SyncDeltaBuilder;
import org.identityconnectors.framework.common.objects.SyncDeltaType;
import org.identityconnectors.framework.common.objects.SyncResultsHandler;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.AttributeNormalizer;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.PoolableConnector;
import org.identityconnectors.framework.spi.SearchResultsHandler;
import org.identityconnectors.framework.spi.SyncTokenResultsHandler;
import org.identityconnectors.framework.spi.operations.AuthenticateOp;
import org.identityconnectors.framework.spi.operations.CreateOp;
import org.identityconnectors.framework.spi.operations.DeleteOp;
import org.identityconnectors.framework.spi.operations.ResolveUsernameOp;
import org.identityconnectors.framework.spi.operations.SchemaOp;
import org.identityconnectors.framework.spi.operations.ScriptOnConnectorOp;
import org.identityconnectors.framework.spi.operations.ScriptOnResourceOp;
import org.identityconnectors.framework.spi.operations.SearchOp;
import org.identityconnectors.framework.spi.operations.SyncOp;
import org.identityconnectors.framework.spi.operations.TestOp;
import org.identityconnectors.framework.spi.operations.UpdateAttributeValuesOp;
import org.identityconnectors.framework.spi.operations.UpdateOp;
import org.identityconnectors.common.security.GuardedString; 
import org.identityconnectors.common.security.GuardedString;   

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Main implementation of the kafka Connector.
 *
 */
@ConnectorClass(
        displayNameKey = "kafka.connector.display",
        configurationClass = kafkaConfiguration.class)
public class kafkaConnector implements Connector, PoolableConnector, AuthenticateOp, CreateOp, DeleteOp, ResolveUsernameOp, SchemaOp, ScriptOnConnectorOp, ScriptOnResourceOp, SearchOp<String>, SyncOp, TestOp, UpdateAttributeValuesOp {

    /**
     * Setup logging for the {@link kafkaConnector}.
     */
    private static final Log logger = Log.getLog(kafkaConnector.class);

    /**
     * Place holder for the {@link Configuration} passed into the init() method
     * {@link kafkaConnector#init(org.identityconnectors.framework.spi.Configuration)}.
     */
    private kafkaConfiguration configuration;
    private KafkaConsumer consumer;

    private Schema schema = null;

    /**
     * Gets the Configuration context for this connector.
     *
     * @return The current {@link Configuration}
     */
    public Configuration getConfiguration() {
        return this.configuration;
    }



    /**
     * Callback method to receive the {@link Configuration}.
     *
     * @param configuration the new {@link Configuration}
     * @see org.identityconnectors.framework.spi.Connector#init(org.identityconnectors.framework.spi.Configuration)
     */
    public void init(final Configuration kafkaconfig) {

        kafkaConfiguration configuration = (kafkaConfiguration) kafkaconfig;

        String clientId=configuration.getClientId();
        String groupId=configuration.getGroupId();
        String host=configuration.getHost();
        String topic=configuration.getTopic();

        GuardedStringAccessor accessor = new GuardedStringAccessor();
        configuration.getPassword().access(accessor);
            String password = new String(accessor.getArray());
            accessor.clear();
            
            configuration.getRemoteUser().access(accessor);
            String remoteuser = new String(accessor.getArray());
            accessor.clear();
        
        String connection="org.apache.kafka.common.security.plain.PlainLoginModule required username='"+remoteuser+"' password='"+password+"';";
        
        System.out.println("system config : "+ clientId + "," +groupId+ "," +host+ "," +topic+ "," +connection);

        this.configuration = (kafkaConfiguration) configuration;

        Properties config = new Properties();
        config.put("client.id", clientId);
        config.put("group.id", groupId);
        config.put("bootstrap.servers", host);
        config.put("sasl.jaas.config",connection);
        config.put("security.protocol","SASL_SSL");
        config.put("ssl.endpoint.identification.algorithm","");
        config.put("sasl.mechanism","PLAIN");
        config.put("client.dns.lookup","use_all_dns_ips");
        
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("session.timeout.ms", 50000);
        this.consumer = new KafkaConsumer<String,String>(config);
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    /**
     * Disposes of the {@link kafkaConnector}'s resources.
     *
     * @see org.identityconnectors.framework.spi.Connector#dispose()
     */
    public void dispose() {
        this.consumer.close();
    }


    public void checkAlive() {
        this.consumer.listTopics();
      }





    /******************
     * SPI Operations
     *
     * Implement the following operations using the contract and
     * description found in the Javadoc for these methods.
     ******************/

    /**
     * {@inheritDoc}
     */
    public Uid authenticate(final ObjectClass objectClass, final String userName,
            final GuardedString password, final OperationOptions options) {
        if (ObjectClass.ACCOUNT.equals(objectClass)) {
            return new Uid(userName);
        } else {
            logger.warn("Authenticate of type {0} is not supported", configuration
                    .getConnectorMessages().format(objectClass.getDisplayNameKey(),
                            objectClass.getObjectClassValue()));
            throw new UnsupportedOperationException("Authenticate of type"
                    + objectClass.getObjectClassValue() + " is not supported");
        }
    }

    /**
     * {@inheritDoc}
     */
    public Uid resolveUsername(final ObjectClass objectClass, final String userName,
            final OperationOptions options) {
        if (ObjectClass.ACCOUNT.equals(objectClass)) {
            return new Uid(userName);
        } else {
            logger.warn("ResolveUsername of type {0} is not supported", configuration
                    .getConnectorMessages().format(objectClass.getDisplayNameKey(),
                            objectClass.getObjectClassValue()));
            throw new UnsupportedOperationException("ResolveUsername of type"
                    + objectClass.getObjectClassValue() + " is not supported");
        }
    }

    /**
     * {@inheritDoc}
     */
    public Uid create(final ObjectClass objectClass, final Set<Attribute> createAttributes,
            final OperationOptions options) {
        if (ObjectClass.ACCOUNT.equals(objectClass) || ObjectClass.GROUP.equals(objectClass)) {
            Name name = AttributeUtil.getNameFromAttributes(createAttributes);
            if (name != null) {
                // do real create here
                return new Uid(AttributeUtil.getStringValue(name).toLowerCase(Locale.US));
            } else {
                throw new InvalidAttributeValueException("Name attribute is required");
            }
        } else {
            logger.warn("Create of type {0} is not supported", configuration.getConnectorMessages()
                    .format(objectClass.getDisplayNameKey(), objectClass.getObjectClassValue()));
            throw new UnsupportedOperationException("Create of type"
                    + objectClass.getObjectClassValue() + " is not supported");
        }
    }

    /**
     * {@inheritDoc}
     */
    public void delete(final ObjectClass objectClass, final Uid uid, final OperationOptions options) {
        if (ObjectClass.ACCOUNT.equals(objectClass) || ObjectClass.GROUP.equals(objectClass)) {
            // do real delete here
        } else {
            logger.warn("Delete of type {0} is not supported", configuration.getConnectorMessages()
                    .format(objectClass.getDisplayNameKey(), objectClass.getObjectClassValue()));
            throw new UnsupportedOperationException("Delete of type"
                    + objectClass.getObjectClassValue() + " is not supported");
        }
    }

    /**
     * {@inheritDoc}
     */
    public Schema schema() {
        if (null == schema) {
            final SchemaBuilder builder = new SchemaBuilder(kafkaConnector.class);
            // Account
            ObjectClassInfoBuilder accountInfoBuilder = new ObjectClassInfoBuilder();
            accountInfoBuilder.addAttributeInfo(Name.INFO);
            accountInfoBuilder.addAttributeInfo(OperationalAttributeInfos.PASSWORD);
            accountInfoBuilder.addAttributeInfo(PredefinedAttributeInfos.GROUPS);
            accountInfoBuilder.addAttributeInfo(AttributeInfoBuilder.build("firstName"));
            accountInfoBuilder.addAttributeInfo(AttributeInfoBuilder.define("lastName")
                    .setRequired(true).build());
            builder.defineObjectClass(accountInfoBuilder.build());

            // Group
            ObjectClassInfoBuilder groupInfoBuilder = new ObjectClassInfoBuilder();
            groupInfoBuilder.setType(ObjectClass.GROUP_NAME);
            groupInfoBuilder.addAttributeInfo(Name.INFO);
            groupInfoBuilder.addAttributeInfo(PredefinedAttributeInfos.DESCRIPTION);
            groupInfoBuilder.addAttributeInfo(AttributeInfoBuilder.define("members").setCreateable(
                    false).setUpdateable(false).setMultiValued(true).build());

            // Only the CRUD operations
            builder.defineObjectClass(groupInfoBuilder.build(), CreateOp.class, SearchOp.class,
                    UpdateOp.class, DeleteOp.class);

            // Operation Options
            builder.defineOperationOption(OperationOptionInfoBuilder.buildAttributesToGet(),
                    SearchOp.class);

            // Support paged Search
            builder.defineOperationOption(OperationOptionInfoBuilder.buildPageSize(),
                    SearchOp.class);
            builder.defineOperationOption(OperationOptionInfoBuilder.buildPagedResultsCookie(),
                    SearchOp.class);

            // Support to execute operation with provided credentials
            builder.defineOperationOption(OperationOptionInfoBuilder.buildRunWithUser());
            builder.defineOperationOption(OperationOptionInfoBuilder.buildRunWithPassword());

            schema = builder.build();
        }
        return schema;
    }

    /**
     * {@inheritDoc}
     */
    public Object runScriptOnConnector(ScriptContext request, OperationOptions options) {
        final ScriptExecutorFactory factory =
                ScriptExecutorFactory.newInstance(request.getScriptLanguage());
        final ScriptExecutor executor =
                factory.newScriptExecutor(getClass().getClassLoader(), request.getScriptText(),
                        true);

        if (StringUtil.isNotBlank(options.getRunAsUser())) {
            String password = SecurityUtil.decrypt(options.getRunWithPassword());
            // Use these to execute the script with these credentials
            Assertions.blankCheck(password, "password");
        }
        try {
            return executor.execute(request.getScriptArguments());
        } catch (Throwable e) {
            logger.warn(e, "Failed to execute Script");
            throw ConnectorException.wrap(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public Object runScriptOnResource(ScriptContext request, OperationOptions options) {
        try {
            // Execute the script on remote resource
            if (StringUtil.isNotBlank(options.getRunAsUser())) {
                String password = SecurityUtil.decrypt(options.getRunWithPassword());
                // Use these to execute the script with these credentials
                Assertions.blankCheck(password, "password");
                return options.getRunAsUser();
            }
            throw new UnknownHostException("Failed to connect to remote SSH");
        } catch (Throwable e) {
            logger.warn(e, "Failed to execute Script");
            throw ConnectorException.wrap(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public FilterTranslator<String> createFilterTranslator(ObjectClass objectClass,
            OperationOptions options) {
        return new kafkaFilterTranslator();
    }

    /**
     * {@inheritDoc}
     */
    public void executeQuery(ObjectClass objectClass, String query, ResultsHandler handler,
            OperationOptions options) {

            System.out.println("Entering executequery:"+objectClass+" , "+query+ " , "+options);
                
               try{
                   ConsumerRecords<String,String> records = this.consumer.poll(Duration.ofMillis(10000));
                   System.out.println("records recieved: "+records.count());
                    for (ConsumerRecord<String,String> record : records) {
                       String key = record.key();
                       String value = record.value();
                       System.out.printf("recon Received Message topic =%s, partition =%s, offset = %d, key = %s, value = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                       final ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
                       AttributeBuilder attributeBuilder = new AttributeBuilder();
                       builder.setObjectClass(objectClass);
                       builder.setUid(key);
                       builder.setName(key);
                       builder.addAttribute("firstName",value);
                       builder.addAttribute("lastName",value);
                       builder.addAttribute("email",value);
                         
                       ConnectorObject connectorObject = builder.build();
                       if (!handler.handle(connectorObject)) {
                        // Stop iterating because the handler stopped processing
                        break;
                    }
    
                   }
                   consumer.commitSync();
                  }
                 catch (Exception e) {
                     System.out.println(e);
                  }
    }

    /**
     * {@inheritDoc}
     */
    public void sync(ObjectClass objectClass, SyncToken token, SyncResultsHandler handler,
            final OperationOptions options) {
                
        System.out.println("Entering sync:"+objectClass+" , "+options);
        
            
        
        // if (ObjectClass.ALL.equals(objectClass)) {
        //     // no action
        // } else if (objectClass.toString()=="Account") {
               
            try{            
                ConsumerRecords<String,String> records = this.consumer.poll(Duration.ofMillis(10000));
                logger.info("Total records:"+records.count());
                for (ConsumerRecord<String,String> record : records) {
                   String key = record.key();
                   String value = record.value();
                   System.out.printf("Sync Received Message topic =%s, partition =%s, offset = %d, key = %s, value = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());


                   Uid uid = new Uid(key);

                    final ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
                    builder.setObjectClass(objectClass);
                    builder.setUid(uid);
                    builder.setName(key);
                    builder.addAttribute("firstName",value);
                    builder.addAttribute("lastName",value);
                    builder.addAttribute("email",value+"@testmail.com");
                        
       
                   final SyncDeltaBuilder deltaBuilder = new SyncDeltaBuilder();
                   deltaBuilder.setObjectClass(objectClass);
                   deltaBuilder.setUid(uid);
                   deltaBuilder.setObject(builder.build());
                   deltaBuilder.setDeltaType(SyncDeltaType.CREATE);
                   deltaBuilder.setToken(new SyncToken(record.offset()));

                   SyncDelta connectorObject = deltaBuilder.build();
                   System.out.println("handling object1:"+connectorObject);
                   if (!handler.handle(connectorObject)) {
                    // Stop iterating because the handler stopped processing
                    System.out.println("handling object2:"+connectorObject);
                    break;
                }

               }
               consumer.commitSync();
              }
             catch (Exception e) {
                 System.out.println(e);
              }
             

        // } else {
        //     logger.warn("Sync of type {0} is not supported", configuration.getConnectorMessages()
        //             .format(objectClass.getDisplayNameKey(), objectClass.getObjectClassValue()));
        //     throw new UnsupportedOperationException("Sync of type"
        //             + objectClass.getObjectClassValue() + " is not supported");
        // }
        ((SyncTokenResultsHandler) handler).handleResult(new SyncToken(10));
    }

    /**
     * {@inheritDoc}
     */
    public SyncToken getLatestSyncToken(ObjectClass objectClass) {
        return new SyncToken(0);
    }

    /**
     * {@inheritDoc}
     */
    public void test() {
        logger.ok("Test works well");
    }

    /**
     * {@inheritDoc}
     */
    public Uid update(ObjectClass objectClass, Uid uid, Set<Attribute> replaceAttributes,
            OperationOptions options) {
        AttributesAccessor attributesAccessor = new AttributesAccessor(replaceAttributes);
        Name newName = attributesAccessor.getName();
        Uid uidAfterUpdate = uid;
        if (newName != null) {
            logger.info("Rename the object {0}:{1} to {2}", objectClass.getObjectClassValue(), uid
                    .getUidValue(), newName.getNameValue());
            uidAfterUpdate = new Uid(newName.getNameValue().toLowerCase(Locale.US));
        }

        if (ObjectClass.ACCOUNT.equals(objectClass)) {

        } else if (ObjectClass.GROUP.is(objectClass.getObjectClassValue())) {
            if (attributesAccessor.hasAttribute("members")) {
                throw new InvalidAttributeValueException(
                        "Requested to update a read only attribute");
            }
        } else {
            logger.warn("Update of type {0} is not supported", configuration.getConnectorMessages()
                    .format(objectClass.getDisplayNameKey(), objectClass.getObjectClassValue()));
            throw new UnsupportedOperationException("Update of type"
                    + objectClass.getObjectClassValue() + " is not supported");
        }
        return uidAfterUpdate;
    }

    /**
     * {@inheritDoc}
     */
    public Uid addAttributeValues(ObjectClass objectClass, Uid uid, Set<Attribute> valuesToAdd,
            OperationOptions options) {
        return uid;
    }

    /**
     * {@inheritDoc}
     */
    public Uid removeAttributeValues(ObjectClass objectClass, Uid uid,
            Set<Attribute> valuesToRemove, OperationOptions options) {
        return uid;
    }
}
