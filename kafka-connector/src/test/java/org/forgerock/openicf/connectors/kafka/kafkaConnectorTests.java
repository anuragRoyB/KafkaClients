/*
 * Copyright 2016-2017 ForgeRock AS. All Rights Reserved
 *
 * Use of this code requires a commercial software license with ForgeRock AS.
 * or with one of its affiliates. All use shall be exclusively subject
 * to such license between the licensee and ForgeRock AS.
 */

package org.forgerock.openicf.connectors.kafka;

import java.util.HashSet;
import java.util.Set;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.APIConfiguration;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.api.ConnectorFacadeFactory;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptionsBuilder;
import org.identityconnectors.framework.common.objects.PredefinedAttributes;
import org.identityconnectors.framework.common.objects.ResultsHandler;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.ScriptContextBuilder;
import org.identityconnectors.framework.common.objects.SearchResult;
import org.identityconnectors.framework.common.objects.SyncDelta;
import org.identityconnectors.framework.common.objects.SyncResultsHandler;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.FilterBuilder;
import org.identityconnectors.framework.impl.api.local.LocalConnectorFacadeImpl;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.test.common.PropertyBag;
import org.identityconnectors.test.common.TestHelpers;
import org.identityconnectors.test.common.ToListResultsHandler;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


/**
 * Attempts to test the {@link kafkaConnector} with the framework.
 *
 */
public class kafkaConnectorTests {

    /**
    * Setup logging for the {@link kafkaConnectorTests}.
    */
    private static final Log logger = Log.getLog(kafkaConnectorTests.class);

    private ConnectorFacade connectorFacade = null;

    /*
    * Example test properties.
    * See the Javadoc of the TestHelpers class for the location of the public and private configuration files.
    */
    private static final PropertyBag PROPERTIES = TestHelpers.getProperties(kafkaConnector.class);

    @BeforeClass
    public void setUp() {
        //
        //other setup work to do before running tests
        //

        //Configuration config = new kafkaConfiguration();
        //Map<String, ? extends Object> configData = (Map<String, ? extends Object>) PROPERTIES.getProperty("configuration",Map.class)
        //TestHelpers.fillConfiguration(
    }

    @AfterClass
    public void tearDown() {
        //
        // clean up resources
        //
        if (connectorFacade instanceof LocalConnectorFacadeImpl) {
            ((LocalConnectorFacadeImpl) connectorFacade).dispose();
        }
    }

    @Test
    public void exampleTest1() {
        logger.info("Running Test 1...");
        //You can use TestHelpers to do some of the boilerplate work in running a search
        //TestHelpers.search(theConnector, ObjectClass.ACCOUNT, filter, handler, null);
    }

    @Test
    public void exampleTest2() {
        logger.info("Running Test 2...");
        System.out.println("helloooo");
        Properties config = new Properties();
        config.put("client.id", "test");
        config.put("group.id", "foo");
        config.put("bootstrap.servers", "pkc-419q3.us-east4.gcp.confluent.cloud:9092");
        config.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='XBUMPW75LOP3HHOA' password='z/vMao4rzhqScUBhoK3EDi7zolrJK/hH9WL18wah+5pl+kEE2eOLqhoAkwvgoHtK';");
        config.put("security.protocol","SASL_SSL");
        config.put("ssl.endpoint.identification.algorithm","");
        config.put("sasl.mechanism","PLAIN");
        config.put("client.dns.lookup","use_all_dns_ips");
        config.put("acks","all");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("session.timeout.ms", 50000);

        System.out.println("reading kafka1");
          
        final KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(config);
        System.out.println("reading kafka2");

        System.out.println(consumer.listTopics());
        
        consumer.subscribe(Collections.singletonList("testfrim"));
        System.out.println("reading kafka3");
        try{
            
         ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(10000));
         System.out.println("rcords:"+records.count());
         for (ConsumerRecord<String,String> record : records) {
            String key = record.key();
            String value = record.value();
            System.out.println("Consumed record with key "+key+" and value " + value);
        }
        
       }
      catch (Exception e) {
          System.out.println(e);
       }

    System.out.println("test finished");

    //Another example using TestHelpers
    //List<ConnectorObject> results = TestHelpers.searchToList(theConnector, ObjectClass.GROUP, filter);
    }

/*

    @Test
    public void authenticateTest() {
        logger.info("Running Authentication Test");
        final ConnectorFacade facade = getFacade(kafkaConnector.class, null);
        final OperationOptionsBuilder builder = new OperationOptionsBuilder();
        Uid uid =
                facade.authenticate(ObjectClass.ACCOUNT, "username", new GuardedString("Passw0rd"
                        .toCharArray()), builder.build());
        Assert.assertEquals(uid.getUidValue(), "username");
    }

    @Test
    public void createTest() {
        logger.info("Running Create Test");
        final ConnectorFacade facade = getFacade(kafkaConnector.class, null);
        final OperationOptionsBuilder builder = new OperationOptionsBuilder();
        Set<Attribute> createAttributes = new HashSet<Attribute>();
        createAttributes.add(new Name("Foo"));
        createAttributes.add(AttributeBuilder.buildPassword("Password".toCharArray()));
        createAttributes.add(AttributeBuilder.buildEnabled(true));
        Uid uid = facade.create(ObjectClass.ACCOUNT, createAttributes, builder.build());
        Assert.assertEquals(uid.getUidValue(), "foo");
    }

    @Test
    public void deleteTest() {
        logger.info("Running Delete Test");
        final ConnectorFacade facade = getFacade(kafkaConnector.class, null);
        final OperationOptionsBuilder builder = new OperationOptionsBuilder();
        facade.delete(ObjectClass.ACCOUNT, new Uid("username"), builder.build());
    }

    @Test
    public void resolveUsernameTest() {
        logger.info("Running ResolveUsername Test");
        final ConnectorFacade facade = getFacade(kafkaConnector.class, null);
        final OperationOptionsBuilder builder = new OperationOptionsBuilder();
        Uid uid = facade.resolveUsername(ObjectClass.ACCOUNT, "username", builder.build());
        Assert.assertEquals(uid.getUidValue(), "username");
    }

    @Test
    public void schemaTest() {
        logger.info("Running Schema Test");
        final ConnectorFacade facade = getFacade(kafkaConnector.class, null);
        Schema schema = facade.schema();
        Assert.assertNotNull(schema.findObjectClassInfo(ObjectClass.ACCOUNT_NAME));
    }

    @Test
    public void runScriptOnConnectorTest() {
        logger.info("Running RunScriptOnConnector Test");
        final ConnectorFacade facade = getFacade(kafkaConnector.class, null);
        final OperationOptionsBuilder builder = new OperationOptionsBuilder();
        builder.setRunAsUser("admin");
        builder.setRunWithPassword(new GuardedString("Passw0rd".toCharArray()));

        final ScriptContextBuilder scriptBuilder =
                new ScriptContextBuilder("Groovy", "return argument");
        scriptBuilder.addScriptArgument("argument", "value");

        Object result = facade.runScriptOnConnector(scriptBuilder.build(), builder.build());
        Assert.assertEquals(result, "value");
    }

    @Test
    public void runScriptOnResourceTest() {
        logger.info("Running RunScriptOnResource Test");
        final ConnectorFacade facade = getFacade(kafkaConnector.class, null);
        final OperationOptionsBuilder builder = new OperationOptionsBuilder();
        builder.setRunAsUser("admin");
        builder.setRunWithPassword(new GuardedString("Passw0rd".toCharArray()));

        final ScriptContextBuilder scriptBuilder = new ScriptContextBuilder("bash", "whoami");

        Object result = facade.runScriptOnResource(scriptBuilder.build(), builder.build());
        Assert.assertEquals(result, "admin");
    }

    @Test
    public void getObjectTest() {
        logger.info("Running GetObject Test");
        final ConnectorFacade facade = getFacade(kafkaConnector.class, null);
        final OperationOptionsBuilder builder = new OperationOptionsBuilder();
        builder.setAttributesToGet(Name.NAME);
        ConnectorObject co =
                facade.getObject(ObjectClass.ACCOUNT, new Uid(
                        "3f50eca0-f5e9-11e3-a3ac-0800200c9a66"), builder.build());
        Assert.assertEquals(co.getName().getNameValue(), "Foo");
    }

    @Test
    public void searchTest() {
        logger.info("Running Search Test");
        final ConnectorFacade facade = getFacade(kafkaConnector.class, null);
        final OperationOptionsBuilder builder = new OperationOptionsBuilder();
        builder.setPageSize(10);
        final ResultsHandler handler = new ToListResultsHandler();

        SearchResult result =
                facade.search(ObjectClass.ACCOUNT, FilterBuilder.equalTo(new Name("Foo")), handler,
                        builder.build());
        Assert.assertEquals(result.getPagedResultsCookie(), "0");
        Assert.assertEquals(((ToListResultsHandler) handler).getObjects().size(), 1);
    }

    @Test
    public void getLatestSyncTokenTest() {
        logger.info("Running GetLatestSyncToken Test");
        final ConnectorFacade facade = getFacade(kafkaConnector.class, null);
        SyncToken token = facade.getLatestSyncToken(ObjectClass.ACCOUNT);
        Assert.assertEquals(token.getValue(), 10);
    }

    @Test
    public void syncTest() {
        logger.info("Running Sync Test");
        final ConnectorFacade facade = getFacade(kafkaConnector.class, null);
        final OperationOptionsBuilder builder = new OperationOptionsBuilder();
        builder.setPageSize(10);
        final SyncResultsHandler handler = new SyncResultsHandler() {
            public boolean handle(SyncDelta delta) {
                return false;
            }
        };

        SyncToken token =
                facade.sync(ObjectClass.ACCOUNT, new SyncToken(10), handler, builder.build());
        Assert.assertEquals(token.getValue(), 10);
    }

    @Test
    public void testTest() {
        logger.info("Running Test Test");
        final ConnectorFacade facade = getFacade(kafkaConnector.class, null);
        facade.test();
    }

    @Test
    public void validateTest() {
        logger.info("Running Validate Test");
        final ConnectorFacade facade = getFacade(kafkaConnector.class, null);
        facade.validate();
    }

    @Test
    public void updateTest() {
        logger.info("Running Update Test");
        final ConnectorFacade facade = getFacade(kafkaConnector.class, null);
        final OperationOptionsBuilder builder = new OperationOptionsBuilder();
        Set<Attribute> updateAttributes = new HashSet<Attribute>();
        updateAttributes.add(new Name("Foo"));

        Uid uid = facade.update(ObjectClass.ACCOUNT, new Uid("Foo"), updateAttributes, builder.build());
        Assert.assertEquals(uid.getUidValue(), "foo");
    }

    @Test
    public void addAttributeValuesTest() {
        logger.info("Running AddAttributeValues Test");
        final ConnectorFacade facade = getFacade(kafkaConnector.class, null);
        final OperationOptionsBuilder builder = new OperationOptionsBuilder();
        Set<Attribute> updateAttributes = new HashSet<Attribute>();
        // add 'group2' to existing groups
        updateAttributes.add(AttributeBuilder.build(PredefinedAttributes.GROUPS_NAME, "group2"));

        Uid uid = facade.addAttributeValues(ObjectClass.ACCOUNT, new Uid("Foo"), updateAttributes, builder.build());
        Assert.assertEquals(uid.getUidValue(), "foo");
    }

    @Test
    public void removeAttributeValuesTest() {
        logger.info("Running RemoveAttributeValues Test");
        final ConnectorFacade facade = getFacade(kafkaConnector.class, null);
        final OperationOptionsBuilder builder = new OperationOptionsBuilder();
        Set<Attribute> updateAttributes = new HashSet<Attribute>();
        // remove 'group2' from existing groups
        updateAttributes.add(AttributeBuilder.build(PredefinedAttributes.GROUPS_NAME, "group2"));

        Uid uid = facade.removeAttributeValues(ObjectClass.ACCOUNT, new Uid("Foo"), updateAttributes, builder.build());
        Assert.assertEquals(uid.getUidValue(), "foo");
    }
*/
    protected ConnectorFacade getFacade(kafkaConfiguration config) {
        ConnectorFacadeFactory factory = ConnectorFacadeFactory.getInstance();
        // **test only**
        APIConfiguration impl = TestHelpers.createTestConfiguration(kafkaConnector.class, config);
        return factory.newInstance(impl);
    }

    protected ConnectorFacade getFacade(Class<? extends Connector> clazz, String environment) {
        if (null == connectorFacade) {
            synchronized (this) {
                if (null == connectorFacade) {
                    connectorFacade = createConnectorFacade(clazz, environment);
                }
            }
        }
        return connectorFacade;
    }

    public ConnectorFacade createConnectorFacade(Class<? extends Connector> clazz,
        String environment) {
        PropertyBag propertyBag = TestHelpers.getProperties(clazz, environment);

        APIConfiguration impl =
            TestHelpers.createTestConfiguration(clazz, propertyBag, "configuration");
        impl.setProducerBufferSize(0);
        impl.getResultsHandlerConfiguration().setEnableAttributesToGetSearchResultsHandler(false);
        impl.getResultsHandlerConfiguration().setEnableCaseInsensitiveFilter(false);
        impl.getResultsHandlerConfiguration().setEnableFilteredResultsHandler(false);
        impl.getResultsHandlerConfiguration().setEnableNormalizingResultsHandler(false);

        //impl.setTimeout(CreateApiOp.class, 25000);
        //impl.setTimeout(UpdateApiOp.class, 25000);
        //impl.setTimeout(DeleteApiOp.class, 25000);

        return ConnectorFacadeFactory.getInstance().newInstance(impl);
    }
}
