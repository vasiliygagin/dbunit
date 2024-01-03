/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.dataset.xml;

import static org.dbunit.dataset.xml.XmlUtil.buildInputSourceFromContent;
import static org.dbunit.dataset.xml.XmlUtil.buildInputSourceFromFile;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetConsumerMockVerifyer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.junit.Test;

public class FlatDtdProducerTest {

    @Test
    public void testSequenceModel() throws Exception {
        String content = //
                "<!ELEMENT dataset (DUPLICATE_TABLE*,TEST_TABLE+,DUPLICATE_TABLE?)>" //
                        + "<!ELEMENT TEST_TABLE EMPTY>" //
                        + "<!ELEMENT DUPLICATE_TABLE EMPTY>";
        FlatDtdProducer producer = new FlatDtdProducer(buildInputSourceFromContent(content));
        IDataSetConsumer consumer = mock(IDataSetConsumer.class);

        producer.produce(consumer);

        IDataSetConsumerMockVerifyer verifyer = new IDataSetConsumerMockVerifyer(consumer);
        verifyer.verifyStartDataSet();
        verifyer.verifyDtdTable("DUPLICATE_TABLE");
        verifyer.verifyDtdTable("TEST_TABLE");
        verifyer.verifyDtdTable("DUPLICATE_TABLE");
        verifyer.verifyEndDataSet();
        verifyer.verifyNoMoreInvocations();
    }

    @Test
    public void testChoicesModel() throws Exception {
        String content = //
                "<!ELEMENT dataset (TEST_TABLE|SECOND_TABLE)>" //
                        + "<!ELEMENT TEST_TABLE EMPTY>" //
                        + "<!ELEMENT SECOND_TABLE EMPTY>";
        FlatDtdProducer producer = new FlatDtdProducer(buildInputSourceFromContent(content));
        IDataSetConsumer consumer = mock(IDataSetConsumer.class);

        producer.produce(consumer);

        IDataSetConsumerMockVerifyer verifyer = new IDataSetConsumerMockVerifyer(consumer);
        verifyer.verifyStartDataSet();
        verifyer.verifyDtdTable("TEST_TABLE");
        verifyer.verifyDtdTable("SECOND_TABLE");
        verifyer.verifyEndDataSet();
        verifyer.verifyNoMoreInvocations();
    }

    @Test
    public void testChoicesModel_ElementDeclarationForTableMissing() throws Exception {
        String content = //
                "<!ELEMENT dataset ( (TEST_TABLE|SECOND_TABLE)* )>" //
                        + "<!ELEMENT TEST_TABLE EMPTY>";
        FlatDtdProducer producer = new FlatDtdProducer(buildInputSourceFromContent(content));
        IDataSetConsumer consumer = mock(IDataSetConsumer.class);

        try {
            producer.produce(consumer);
            fail("Should not be able to produce the dataset from an incomplete DTD");
        } catch (DataSetException expected) {
            String expectedStartsWith = "ELEMENT/ATTRIBUTE declaration for '" + "SECOND_TABLE" + "' is missing. ";
            assertTrue(expected.getMessage().startsWith(expectedStartsWith));
        }

        IDataSetConsumerMockVerifyer verifyer = new IDataSetConsumerMockVerifyer(consumer);
        verifyer.verifyStartDataSet();
        verifyer.verifyDtdTable("TEST_TABLE");
        verifyer.verifyNoMoreInvocations();
    }

    @Test
    public void testAttrListBeforeParentElement() throws Exception {
        String content = //
                "<!ELEMENT dataset (TEST_TABLE)>" //
                        + "<!ATTLIST TEST_TABLE " //
                        + "COLUMN0 CDATA #IMPLIED " //
                        + "COLUMN1 CDATA #IMPLIED " //
                        + "COLUMN2 CDATA #IMPLIED " //
                        + "COLUMN3 CDATA #IMPLIED" //
                        + ">" //
                        + "<!ELEMENT TEST_TABLE EMPTY>";
        FlatDtdProducer producer = new FlatDtdProducer(buildInputSourceFromContent(content));
        IDataSetConsumer consumer = mock(IDataSetConsumer.class);

        producer.produce(consumer);

        IDataSetConsumerMockVerifyer verifyer = new IDataSetConsumerMockVerifyer(consumer);
        verifyer.verifyStartDataSet();
        verifyer.verifyDtdTable("TEST_TABLE", "COLUMN0", "COLUMN1", "COLUMN2", "COLUMN3");
        verifyer.verifyEndDataSet();
        verifyer.verifyNoMoreInvocations();
    }

    @Test
    public void testCleanupTableName() throws Exception {
        String content = //
                "<!ELEMENT dataset (TABLE_1,(TABLE_2,TABLE_3+)?)+>" //
                        + "<!ELEMENT TABLE_1 EMPTY>" //
                        + "<!ELEMENT TABLE_2 EMPTY>" //
                        + "<!ELEMENT TABLE_3 EMPTY>";
        FlatDtdProducer producer = new FlatDtdProducer(buildInputSourceFromContent(content));
        IDataSetConsumer consumer = mock(IDataSetConsumer.class);

        producer.produce(consumer);

        IDataSetConsumerMockVerifyer verifyer = new IDataSetConsumerMockVerifyer(consumer);
        verifyer.verifyStartDataSet();
        verifyer.verifyDtdTable("TABLE_1");
        verifyer.verifyDtdTable("TABLE_2");
        verifyer.verifyDtdTable("TABLE_3");
        verifyer.verifyEndDataSet();
        verifyer.verifyNoMoreInvocations();
    }

    @Test
    public void testANYModel() throws Exception {
        String content = //
                "<!ELEMENT dataset ANY>" //
                        + "<!ELEMENT TEST_TABLE EMPTY>" //
                        + "<!ELEMENT SECOND_TABLE EMPTY>";
        FlatDtdProducer producer = new FlatDtdProducer(buildInputSourceFromContent(content));
        IDataSetConsumer consumer = mock(IDataSetConsumer.class);

        producer.produce(consumer);

        IDataSetConsumerMockVerifyer verifyer = new IDataSetConsumerMockVerifyer(consumer);
        verifyer.verifyStartDataSet();
        verifyer.verifyDtdTable("SECOND_TABLE");
        verifyer.verifyDtdTable("TEST_TABLE");
        verifyer.verifyEndDataSet();
        verifyer.verifyNoMoreInvocations();
    }

    @Test
    public void testProduce() throws Exception {
        IDataSetProducer producer = new FlatDtdProducer(
                buildInputSourceFromFile("src/test/resources/dtd/flatDtdProducerTest.dtd"));
        IDataSetConsumer consumer = mock(IDataSetConsumer.class);

        producer.produce(consumer);

        IDataSetConsumerMockVerifyer verifyer = new IDataSetConsumerMockVerifyer(consumer);
        verifyer.verifyStartDataSet();
        verifyer.verifyDtdTable("DUPLICATE_TABLE", "COLUMN0", "COLUMN1", "COLUMN2", "COLUMN3");
        verifyer.verifyDtdTable("SECOND_TABLE", "COLUMN0", "COLUMN1", "COLUMN2", "COLUMN3");
        verifyer.verifyDtdTable("TEST_TABLE", "COLUMN0", "COLUMN1", "COLUMN2", "COLUMN3");
        verifyer.verifyDtdTable("NOT_NULL_TABLE", "COLUMN0", "COLUMN1", "COLUMN2", "COLUMN3");
        verifyer.verifyDtdTable("EMPTY_TABLE", "COLUMN0", "COLUMN1", "COLUMN2", "COLUMN3");
        verifyer.verifyEndDataSet();
        verifyer.verifyNoMoreInvocations();
    }

    @Test
    public void testProduceWithoutConsumer() throws Exception {
        IDataSetProducer producer = new FlatDtdProducer(
                buildInputSourceFromFile("src/test/resources/dtd/flatDtdProducerTest.dtd"));
        producer.produce();
    }
}
