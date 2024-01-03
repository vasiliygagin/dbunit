/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit;

import java.io.FileInputStream;
import java.io.OutputStreamWriter;

import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatDtdWriter;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;

public class FlatDtdWriterTestMain {
    public static void main(String[] args) throws Exception {
        FileInputStream xmlStream = new FileInputStream("src/test/resources/xml/flatXmlDataSetTest.xml");
        IDataSet dataSet = new FlatXmlDataSetBuilder().build(xmlStream);

        FlatDtdWriter dtdWriter = new FlatDtdWriter(new OutputStreamWriter(System.out));
        dtdWriter.write(dataSet);
    }
}
