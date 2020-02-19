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

package org.dbunit.dataset.yaml;

import org.dbunit.Assertion;
import org.dbunit.dataset.AbstractDataSetTest;
import org.dbunit.dataset.DataSetUtils;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.testutil.TestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Björn Beskow
 * @version $Revision$ $Date$
 */
public class YmlDataSetTest extends AbstractDataSetTest
{
    public YmlDataSetTest(String s)
    {
        super(s);
    }

    @Override
    protected IDataSet createDataSet() throws Exception
    {
        InputStream in = new FileInputStream(TestUtils.getFile("yaml/dataSetTest.yml"));
        return new YamlDataSet(in);
    }

    @Override
    protected IDataSet createDuplicateDataSet() throws Exception
    {
        InputStream in = new FileInputStream(
        TestUtils.getFile("yaml/yamlDataSetDuplicateTest.yml"));
        return new YamlDataSet(in);
    }

    protected IDataSet createMultipleCaseDuplicateDataSet() throws Exception
    {
        throw new UnsupportedOperationException("Yaml is always case-sensitive");
    }

    @Override
    public void testCreateMultipleCaseDuplicateDataSet() throws Exception
    {
        // Not applicable, Yaml is always case-sensitive
    }

    @Override
    public void testGetCaseInsensitiveTable() throws Exception
    {
        // Not applicable, Yaml is always case-sensitive
    }

    @Override
    public void testGetCaseInsensitiveTableMetaData() throws Exception
    {
        // Not applicable, Yaml is always case-sensitive
    }

    public void testWrite() throws Exception
    {
        IDataSet expectedDataSet = createDataSet();
        File tempFile = File.createTempFile("dataSetTest", ".yml");
        try
        {
            OutputStream out = new FileOutputStream(tempFile);

            try
            {
                // write dataset in temp file
                YamlDataSet.write(expectedDataSet, out);

                // load new dataset from temp file
                IDataSet actualDataSet =
                new YamlDataSet(new FileInputStream(tempFile));

                // verify table count
                assertEquals("table count",
                expectedDataSet.getTableNames().length,
                actualDataSet.getTableNames().length);

                // verify each table
                ITable[] expected = DataSetUtils.getTables(expectedDataSet);
                ITable[] actual = DataSetUtils.getTables(actualDataSet);
                assertEquals("table count", expected.length, actual.length);
                for (int i = 0; i < expected.length; i++)
                {
                    String expectedName =
                    expected[i].getTableMetaData().getTableName();
                    String actualName =
                    actual[i].getTableMetaData().getTableName();
                    assertEquals("table name", expectedName, actualName);

                    assertTrue("not same instance", expected[i] != actual[i]);
                    Assertion.assertEquals(expected[i], actual[i]);
                }
            } finally
            {
                out.close();
            }
        } finally
        {
            tempFile.delete();
        }
    }

}
