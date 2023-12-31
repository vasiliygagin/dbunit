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
package org.dbunit;

import static org.junit.Assert.assertNotNull;

import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.SortedTable;
import org.dbunit.operation.DatabaseOperation;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Andres Almiray (aalmiray@users.sourceforge.net)
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since 2.2.0
 */
public abstract class AbstractDatabaseTesterIT extends AbstractDatabaseTest {

    protected IDatabaseConnection _connection;
    protected IDatabaseTester _databaseTester;

    protected final Logger logger = LoggerFactory.getLogger(AbstractDatabaseTesterIT.class);

    public AbstractDatabaseTesterIT() throws Exception {
    }

    protected ITable createOrderedTable(String tableName, String orderByColumn) throws Exception {
        return new SortedTable(_connection.createDataSet().getTable(tableName), new String[] { orderByColumn });
    }

    // //////////////////////////////////////////////////////////////////////////
    // TestCase class

    @Before
    public final void setUp() throws Exception {

        assertNotNull("DatabaseTester is not set", getDatabaseTester());
        getDatabaseTester().setSetUpOperation(getSetUpOperation());
        getDatabaseTester().setDataSet(getDataSet());
        getDatabaseTester().onSetup();

        _connection = getDatabaseTester().getConnection();
    }

    @After
    public final void tearDown() throws Exception {

        assertNotNull("DatabaseTester is not set", getDatabaseTester());
        getDatabaseTester().setTearDownOperation(getTearDownOperation());
        getDatabaseTester().setDataSet(getDataSet());
        getDatabaseTester().onTearDown();

        DatabaseOperation.DELETE_ALL.execute(_connection, _connection.createDataSet());

        _connection = null;
    }

    // //////////////////////////////////////////////////////////////////////////

    protected IDataSet getDataSet() throws Exception {
        return environment.getInitDataSet();
    }

    protected DatabaseOperation getSetUpOperation() {
        return DatabaseOperation.CLEAN_INSERT;
    }

    protected DatabaseOperation getTearDownOperation() {
        return DatabaseOperation.NONE;
    }

    protected abstract IDatabaseTester getDatabaseTester() throws Exception;
}
