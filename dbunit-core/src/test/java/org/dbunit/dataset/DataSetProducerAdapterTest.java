/*
 *
 * The DbUnit Database Testing Framework
 * Copyright (C)2002-2004, DbUnit.org
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */
package org.dbunit.dataset;

import static org.mockito.Mockito.mock;

import java.net.MalformedURLException;

import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetConsumerMockVerifyer;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.testutil.TestUtils;
import org.junit.Test;

public class DataSetProducerAdapterTest {

    private FlatXmlDataSet createDataSet() throws MalformedURLException, DataSetException {
        return new FlatXmlDataSetBuilder().build(TestUtils.getFile("xml/flatXmlProducerTest.xml"));
    }

    @Test
    public void testProduce() throws Exception {
        IDataSetConsumer consumer = mock(IDataSetConsumer.class);
        FlatXmlDataSet dataSet = createDataSet();
        DataSetProducerAdapter producer = new DataSetProducerAdapter(dataSet);
        producer.setConsumer(consumer);

        producer.produce();

        IDataSetConsumerMockVerifyer verifyer = new IDataSetConsumerMockVerifyer(consumer);
        verifyer.verifyStartDataSet();

        verifyer.verifyStartTable(dataSet.getTableMetaData("DUPLICATE_TABLE"));
        verifyer.verifyRow("row 0 col 0", "row 0 col 1", "row 0 col 2", "row 0 col 3");
        verifyer.verifyEndTable();

        verifyer.verifyStartTable(dataSet.getTableMetaData("SECOND_TABLE"));
        verifyer.verifyRow("row 0 col 0", "row 0 col 1", "row 0 col 2", "row 0 col 3");
        verifyer.verifyRow("row 1 col 0", "row 1 col 1", "row 1 col 2", "row 1 col 3");
        verifyer.verifyEndTable();

        verifyer.verifyStartTable(dataSet.getTableMetaData("TEST_TABLE"));
        verifyer.verifyRow("row 0 col 0", "row 0 col 1", "row 0 col 2", "row 0 col 3");
        verifyer.verifyRow("row 1 col 0", "row 1 col 1", "row 1 col 2", "row 1 col 3");
        verifyer.verifyRow("row 2 col 0", "row 2 col 1", "row 2 col 2", "row 2 col 3");
        verifyer.verifyEndTable();

        verifyer.verifyStartTable(dataSet.getTableMetaData("NOT_NULL_TABLE"));
        verifyer.verifyRow("row 0 col 0", "row 0 col 1", "row 0 col 2", "row 0 col 3");
        verifyer.verifyEndTable();

        verifyer.verifyStartTable(dataSet.getTableMetaData("EMPTY_TABLE"));
        verifyer.verifyEndTable();

        verifyer.verifyEndDataSet();
        verifyer.verifyNoMoreInvocations();
    }

    @Test
    public void testProduceWithoutConsumer() throws Exception {
        FlatXmlDataSet dataSet = createDataSet();
        DataSetProducerAdapter producer = new DataSetProducerAdapter(dataSet);

        producer.produce();
    }
}
