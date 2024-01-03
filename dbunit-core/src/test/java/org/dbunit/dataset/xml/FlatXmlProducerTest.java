/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.dataset.xml;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.MalformedURLException;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetConsumerMockVerifyer;
import org.junit.Test;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;

public class FlatXmlProducerTest {

    private FlatXmlProducer buildFlatXmlProducer() throws Exception {
        InputSource source = XmlUtil.buildInputSourceFromFile("src/test/resources/xml/flatXmlProducerTest.xml");
        return new FlatXmlProducer(source);
    }

    private FlatXmlProducer buildFlatXmlProducer(String xmlContent) {
        InputSource source = XmlUtil.buildInputSourceFromContent(xmlContent);
        return new FlatXmlProducer(source);
    }

    @Test
    public void testProduceEmptyDataSet() throws Exception {
        IDataSetConsumer consumer = mock(IDataSetConsumer.class);
        String content = "<?xml version=\"1.0\"?><dataset/>";
        FlatXmlProducer producer = buildFlatXmlProducer(content);

        producer.produce(consumer);

        IDataSetConsumerMockVerifyer verifyer = new IDataSetConsumerMockVerifyer(consumer);
        verifyer.verifyStartDataSet();
        verifyer.verifyEndDataSet();
        verifyer.verifyNoMoreInvocations();
    }

    @Test
    public void testProduceNonDataSet() throws Exception {
        IDataSetConsumer consumer = mock(IDataSetConsumer.class);
        String content = "<?xml version=\"1.0\"?><nondataset/>";
        FlatXmlProducer producer = buildFlatXmlProducer(content);

        try {
            producer.produce(consumer);
        } catch (DataSetException exc) {
            assertThat(exc.getMessage(), is("Expected 'dataset' element"));
        }
    }

    @Test
    public void testProduceNoDtd() throws Exception {
        IDataSetConsumer consumer = mock(IDataSetConsumer.class);
        String content = "<?xml version=\"1.0\"?><dataset><EMPTY_TABLE/></dataset>";
        FlatXmlProducer producer = buildFlatXmlProducer(content);

        producer.produce(consumer);

        IDataSetConsumerMockVerifyer verifyer = new IDataSetConsumerMockVerifyer(consumer);
        verifyer.verifyStartDataSet();
        verifyer.verifyStartTable(producer.getTableMetaData("EMPTY_TABLE"));
        verifyer.verifyEndTable();
        verifyer.verifyEndDataSet();
        verifyer.verifyNoMoreInvocations();
    }

    @Test
    public void testProduceIgnoreDtd() throws Exception {
        IDataSetConsumer consumer = mock(IDataSetConsumer.class);
        String content = "<?xml version=\"1.0\"?>" //
                + "<!DOCTYPE dataset SYSTEM \"uri:/dummy.dtd\">" //
                + "<dataset>" //
                + "<EMPTY_TABLE/>" //
                + "</dataset>";
        InputSource source = XmlUtil.buildInputSourceFromContent(content);
        FlatXmlProducer producer = new FlatXmlProducer(source, false);

        producer.produce(consumer);

        IDataSetConsumerMockVerifyer verifyer = new IDataSetConsumerMockVerifyer(consumer);
        verifyer.verifyStartDataSet();
        verifyer.verifyStartTable(producer.getTableMetaData("EMPTY_TABLE"));
        verifyer.verifyEndTable();
        verifyer.verifyEndDataSet();
        verifyer.verifyNoMoreInvocations();
    }

    @Test
    public void testProduceMetaDataSet() throws Exception {
        String tableName = "EMPTY_TABLE";
        ITableMetaData tableMetadata = mock(ITableMetaData.class);
        when(tableMetadata.getTableName()).thenReturn(tableName);
        IDataSet metaDataSet = mock(IDataSet.class);
        when(metaDataSet.getTableMetaData(tableName)).thenReturn(tableMetadata);
        IDataSetConsumer consumer = mock(IDataSetConsumer.class);
        String content = "<?xml version=\"1.0\"?>" //
                + "<!DOCTYPE dataset SYSTEM \"urn:/dummy.dtd\">" //
                + "<dataset>" //
                + "<EMPTY_TABLE/>" //
                + "</dataset>";
        InputSource source = XmlUtil.buildInputSourceFromContent(content);
        FlatXmlProducer producer = new FlatXmlProducer(source, metaDataSet);

        producer.produce(consumer);

        IDataSetConsumerMockVerifyer verifyer = new IDataSetConsumerMockVerifyer(consumer);
        verifyer.verifyStartDataSet();
        verifyer.verifyStartTable(tableMetadata);
        verifyer.verifyEndTable();
        verifyer.verifyEndDataSet();
        verifyer.verifyNoMoreInvocations();
    }

    @Test
    public void testProduceCustomEntityResolver() throws Exception {
        IDataSetConsumer consumer = mock(IDataSetConsumer.class);

        String dtdContent = "<!ELEMENT dataset (EMPTY_TABLE)>" //
                + "<!ATTLIST EMPTY_TABLE " //
                + "COLUMN0 CDATA #IMPLIED " //
                + "COLUMN1 CDATA #IMPLIED " //
                + "COLUMN2 CDATA #IMPLIED " //
                + "COLUMN3 CDATA #IMPLIED>" //
                + "<!ELEMENT TEST_TABLE EMPTY>";
        final InputSource dtdSource = XmlUtil.buildInputSourceFromContent(dtdContent);
        EntityResolver resolver = mock(EntityResolver.class);
        when(resolver.resolveEntity(null, "urn:/dummy.dtd")).thenReturn(dtdSource);

        String xmlContent = "<?xml version=\"1.0\"?>" //
                + "<!DOCTYPE dataset SYSTEM \"urn:/dummy.dtd\">" //
                + "<dataset>" //
                + "<EMPTY_TABLE/>" //
                + "</dataset>";
        InputSource xmlSource = XmlUtil.buildInputSourceFromContent(xmlContent);
        FlatXmlProducer producer = new FlatXmlProducer(xmlSource, resolver);

        producer.produce(consumer);

        IDataSetConsumerMockVerifyer verifyer = new IDataSetConsumerMockVerifyer(consumer);
        verifyer.verifyStartDataSet();
        verifyer.verifyStartTable(producer.getTableMetaData("EMPTY_TABLE"));
        verifyer.verifyEndTable();
        verifyer.verifyEndDataSet();
        verifyer.verifyNoMoreInvocations();
    }

    @Test
    public void testProduceNotWellFormedXml() throws Exception {
        IDataSetConsumer consumer = mock(IDataSetConsumer.class);
        String content = "<?xml version=\"1.0\"?>" //
                + "<dataset>";
        InputSource source = XmlUtil.buildInputSourceFromContent(content);
        FlatXmlProducer producer = new FlatXmlProducer(source);

        try {
            producer.produce(consumer);
            fail("Should not be here!");
        } catch (DataSetException e) {
        }

        IDataSetConsumerMockVerifyer verifyer = new IDataSetConsumerMockVerifyer(consumer);
        verifyer.verifyStartDataSet();
        verifyer.verifyNoMoreInvocations();
    }

    @Test
    public void testProduce() throws Exception {
        IDataSetConsumer consumer = mock(IDataSetConsumer.class);
        FlatXmlProducer producer = buildFlatXmlProducer();

        producer.produce(consumer);

        IDataSetConsumerMockVerifyer verifyer = new IDataSetConsumerMockVerifyer(consumer);
        verifyer.verifyStartDataSet();

        verifyer.verifyStartTable(producer.getTableMetaData("DUPLICATE_TABLE"));
        verifyer.verifyRow("row 0 col 0", "row 0 col 1", "row 0 col 2", "row 0 col 3");
        verifyer.verifyEndTable();

        verifyer.verifyStartTable(producer.getTableMetaData("SECOND_TABLE"));
        verifyer.verifyRow("row 0 col 0", "row 0 col 1", "row 0 col 2", "row 0 col 3");
        verifyer.verifyRow("row 1 col 0", "row 1 col 1", "row 1 col 2", "row 1 col 3");
        verifyer.verifyEndTable();

        verifyer.verifyStartTable(producer.getTableMetaData("TEST_TABLE"));
        verifyer.verifyRow("row 0 col 0", "row 0 col 1", "row 0 col 2", "row 0 col 3");
        verifyer.verifyRow("row 1 col 0", "row 1 col 1", "row 1 col 2", "row 1 col 3");
        verifyer.verifyRow("row 2 col 0", "row 2 col 1", "row 2 col 2", "row 2 col 3");
        verifyer.verifyEndTable();

        verifyer.verifyStartTable(producer.getTableMetaData("NOT_NULL_TABLE"));
        verifyer.verifyRow("row 0 col 0", "row 0 col 1", "row 0 col 2", "row 0 col 3");
        verifyer.verifyEndTable();

        verifyer.verifyStartTable(producer.getTableMetaData("EMPTY_TABLE"));
        verifyer.verifyEndTable();

        verifyer.verifyEndDataSet();
        verifyer.verifyNoMoreInvocations();
    }

    @Test
    public void testProduceWithoutConsumer() throws Exception {
        FlatXmlProducer producer = buildFlatXmlProducer();
        producer.produce();
    }
}
