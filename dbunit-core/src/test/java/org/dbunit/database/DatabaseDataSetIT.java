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

package org.dbunit.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.dbunit.database.metadata.MetadataManager;
import org.dbunit.dataset.AbstractDataSetTest;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.NoSuchTableException;
import org.dbunit.dataset.filter.DefaultColumnFilter;
import org.dbunit.dataset.filter.ITableFilterSimple;
import org.dbunit.util.QualifiedTableName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Feb 18, 2002
 */
public class DatabaseDataSetIT extends AbstractDataSetTest {

    private AbstractDatabaseConnection _connection;

    ////////////////////////////////////////////////////////////////////////////
    // TestCase class

    public DatabaseDataSetIT() throws Exception {
        // TODO Auto-generated constructor stub
    }

    @Before
    public final void setUp() throws Exception {
        _connection = database.getConnection();
    }

    @After
    public final void tearDown() throws Exception {

        _connection = null;
    }

    ////////////////////////////////////////////////////////////////////////////
    // AbstractDataSetTest class

    @Override
    protected String convertString(String str) throws Exception {
        return environment.convertString(str);
    }

    @Override
    protected IDataSet createDataSet() throws Exception {
        return _connection.createDataSet();
    }

    @Override
    protected String[] getExpectedNames() throws Exception {
        return _connection.createDataSet().getTableNames();
    }

    @Override
    protected IDataSet createDuplicateDataSet() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected IDataSet createMultipleCaseDuplicateDataSet() throws Exception {
        throw new UnsupportedOperationException();
    }

    ////////////////////////////////////////////////////////////////////////////
    // Test methods

    @Test
    public void testGetQualifiedTableNames() throws Exception {
        String[] expectedNames = getExpectedNames();

        DatabaseConfig config = new DatabaseConfig();
        config.setQualifiedTableNames(true);
        MetadataManager metadataManager = new MetadataManager(_connection.getConnection(), config, null,
                _connection.getSchema());
        IDatabaseConnection connection = new DatabaseConnection(_connection.getConnection(), config,
                _connection.getSchema(), metadataManager);

        IDataSet dataSet = connection.createDataSet();
        String[] actualNames = dataSet.getTableNames();

        assertEquals("name count", expectedNames.length, actualNames.length);
        for (int i = 0; i < actualNames.length; i++) {
            String expected = new QualifiedTableName(expectedNames[i], _connection.getSchema()).getQualifiedName();
            String actual = actualNames[i];
            assertEquals("name", expected, actual);
        }
    }

    @Test
    public void testGetColumnsAndQualifiedNamesEnabled() throws Exception {
        String tableName = new QualifiedTableName("TEST_TABLE", _connection.getSchema()).getQualifiedName();
        String[] expected = { "COLUMN0", "COLUMN1", "COLUMN2", "COLUMN3" };

        DatabaseConfig config = new DatabaseConfig();
        config.setQualifiedTableNames(true);
        MetadataManager metadataManager = new MetadataManager(_connection.getConnection(), config, null,
                _connection.getSchema());
        IDatabaseConnection connection = new DatabaseConnection(_connection.getConnection(), config,
                _connection.getSchema(), metadataManager);

        ITableMetaData metaData = connection.createDataSet().getTableMetaData(tableName);
        Column[] columns = metaData.getColumns();

        assertEquals("column count", expected.length, columns.length);
        for (int i = 0; i < columns.length; i++) {
            assertEquals("column name", convertString(expected[i]), columns[i].getColumnName());
        }
    }

    @Test
    public void testGetPrimaryKeysAndQualifiedNamesEnabled() throws Exception {
        String tableName = new QualifiedTableName("PK_TABLE", _connection.getSchema()).getQualifiedName();
        String[] expected = { "PK0", "PK1", "PK2" };

        DatabaseConfig config = new DatabaseConfig();
        config.setQualifiedTableNames(true);
        MetadataManager metadataManager = new MetadataManager(_connection.getConnection(), config, null,
                _connection.getSchema());
        IDatabaseConnection connection = new DatabaseConnection(_connection.getConnection(), config,
                _connection.getSchema(), metadataManager);

        ITableMetaData metaData = connection.createDataSet().getTableMetaData(tableName);
        Column[] columns = metaData.getPrimaryKeys();

        assertEquals("column count", expected.length, columns.length);
        for (int i = 0; i < columns.length; i++) {
            assertEquals("column name", convertString(expected[i]), columns[i].getColumnName());
        }
    }

    @Test
    public void testGetPrimaryKeysWithColumnFilters() throws Exception {

        // TODO (felipeal): I don't know if PK_TABLE is a standard JDBC name or if
        // it's HSQLDB specific. Anyway, now that HSQLDB's schema is set on property,
        // we cannot add it as prefix here....
        String tableName = "PK_TABLE";
//        String tableName = DataSetUtils.getQualifiedName(
//                _connection.getSchema(), "PK_TABLE");

        String[] expected = { "PK0", "PK2" };

        DefaultColumnFilter filter = new DefaultColumnFilter();
        filter.includeColumn("PK0");
        filter.includeColumn("PK2");

        DatabaseConfig config = new DatabaseConfig();
        config.setPrimaryKeysFilter(filter);
        MetadataManager metadataManager = new MetadataManager(_connection.getConnection(), config, null,
                _connection.getSchema());
        IDatabaseConnection connection = new DatabaseConnection(_connection.getConnection(), config,
                _connection.getSchema(), metadataManager);

        ITableMetaData metaData = connection.createDataSet().getTableMetaData(tableName);
        Column[] columns = metaData.getPrimaryKeys();

        assertEquals("column count", expected.length, columns.length);
        for (int i = 0; i < columns.length; i++) {
            assertEquals("column name", convertString(expected[i]), columns[i].getColumnName());
        }
    }

//        @Test public void testGetTableNamesAndCaseSensitive() throws Exception
//    {
//        DatabaseMetaData metaData = _connection.getConnection().getMetaData();
//        metaData.
//    }

    @Override
    public void testCreateDuplicateDataSet() throws Exception {
        // Cannot test! Unsupported feature.
    }

    @Override
    public void testCreateMultipleCaseDuplicateDataSet() throws Exception {
        // Cannot test! Unsupported feature.
    }

    @Test
    public void testGetTableThatIsFiltered() throws Exception {
        final String existingTableToFilter = convertString("TEST_TABLE");
        ITableFilterSimple tableFilter = tableName -> {
            if (tableName.equals(existingTableToFilter))
                return false;
            return true;
        };
        IDataSet dataSet = new DatabaseDataSet(_connection, tableFilter);
        try {
            dataSet.getTable(existingTableToFilter);
            fail("Should not be able to retrieve table from dataset that has not been loaded - expected an exception");
        } catch (NoSuchTableException expected) {
            assertEquals(existingTableToFilter, expected.getMessage());
        }
    }

}
