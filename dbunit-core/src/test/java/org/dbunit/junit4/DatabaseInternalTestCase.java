/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit4;

import java.sql.Connection;
import java.sql.SQLException;

import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.junit.DbUnitTestFacade;
import org.dbunit.junit.internal.DbunitTestCaseTestRunner;
import org.dbunit.junit.internal.SqlScriptExecutor;
import org.dbunit.operation.DatabaseOperation;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runner.RunWith;

/**
 * Convenience class for writing JUnit tests with dbunit easily. <br />
 * Note that there are some even more convenient classes available such as
 * {@link DBTestCase}.
 */
@RunWith(DbunitTestCaseTestRunner.class)
public abstract class DatabaseInternalTestCase {

    @Rule
    public final DbUnitTestFacade dbUnit = new DbUnitTestFacade();

    public DatabaseInternalTestCase() {
    }

    /**
     * Returns the test dataset.
     */
    protected IDataSet getDataSet() throws Exception {
        return new DefaultDataSet();
    }

    @Before
    public final void setUpDatabaseTester() throws Exception {
        DatabaseConnection connection = dbUnit.getConnection();
        SqlScriptExecutor.execute(connection, "src/test/resources/sql/hypersonic.sql");

        getSetUpOperation().execute(connection, getDataSet());
    }

    @After
    public void tearDown() throws Exception {
        DatabaseConnection connection = dbUnit.getConnection();
        getTearDownOperation().execute(connection, getDataSet());

        Connection jdbcConnection = dbUnit.getJdbcConnection();
        DbunitTestCaseTestRunner.assertAfter(() -> {
            org.dbunit.DdlExecutor.executeSql(jdbcConnection, "SHUTDOWN IMMEDIATELY");
        });
    }

    protected Connection getJdbcConnection() throws Exception, SQLException {
        IDatabaseConnection connection = getConnection();
        return connection.getConnection();
    }

    protected IDatabaseConnection getConnection() throws Exception {
        return dbUnit.getConnection();
    }

    /**
     * Returns the database operation executed in test setup.
     */
    protected DatabaseOperation getSetUpOperation() throws Exception {
        return DatabaseOperation.CLEAN_INSERT;
    }

    /**
     * Returns the database operation executed in test cleanup.
     */
    protected DatabaseOperation getTearDownOperation() throws Exception {
        return DatabaseOperation.NONE;
    }
}
