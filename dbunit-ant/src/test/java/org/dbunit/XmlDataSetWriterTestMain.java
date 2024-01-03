/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.MockDataSetProducer;
import org.dbunit.dataset.stream.StreamingDataSet;
import org.dbunit.dataset.xml.XmlDataSetWriter;

public class XmlDataSetWriterTestMain {

    public static void main(String[] args) throws Exception {
        MockDataSetProducer mockProducer = new MockDataSetProducer();
        mockProducer.setupColumnCount(5);
        mockProducer.setupRowCount(100000);
        mockProducer.setupTableCount(10);
        IDataSet dataSet = new StreamingDataSet(mockProducer);

        OutputStream out = new FileOutputStream("xmlWriterTest.xml");
        XmlDataSetWriter writer = new XmlDataSetWriter(new OutputStreamWriter(out, "UTF8"));
        writer.write(dataSet);
    }
}
