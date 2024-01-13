/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit4;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;

import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.junit.DbUnitTestFacade;
import org.dbunit.junit.internal.DbunitTestCaseTestRunner;
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

    private DefaultDatabaseTester databaseTester;

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
        final File ddlFile = new File("src/test/resources/sql/hypersonic.sql").getAbsoluteFile();
        DdlExecutor.executeDdl(dbUnit.getJdbcConnection(), ddlFile);

        DatabaseConnection connection = dbUnit.getConnection();
        databaseTester = new DefaultDatabaseTester(connection);
        databaseTester.setOperationListener(new DefaultOperationListener() {

            @Override
            public void operationSetUpFinished(IDatabaseConnection connection) {
                // Ugly prevent close.
                // Need to teach Database Tester to get / release connections from ConnectionSource
            }

            @Override
            public void operationTearDownFinished(IDatabaseConnection connection) {
                // Ugly prevent close.
                // Need to teach Database Tester to get / release connections from ConnectionSource
            }
        });

        databaseTester.setSetUpOperation(getSetUpOperation());
        databaseTester.setDataSet(getDataSet());

        databaseTester.onSetup();
    }

    @After
    public void tearDown() throws Exception {
        databaseTester.setTearDownOperation(getTearDownOperation());
        databaseTester.setDataSet(getDataSet());

        databaseTester.onTearDown();
        Connection jdbcConnection = dbUnit.getJdbcConnection();
        DbunitTestCaseTestRunner.assertAfter(() -> {
            org.dbunit.DdlExecutor.executeSql(jdbcConnection, "SHUTDOWN IMMEDIATELY");
        });
    }

    /**
     * Creates a IDatabaseTester for this testCase.<br>
     *
     * A {@link DefaultDatabaseTester} is used by default.
     *
     * @throws Exception
     */
    protected IDatabaseTester buildDatabaseTester() throws Exception {
        DatabaseConnection connection = dbUnit.getConnection();
        return new DefaultDatabaseTester(connection);
    }

    protected Connection getJdbcConnection() throws Exception, SQLException {
        IDatabaseConnection connection = getConnection();
        return connection.getConnection();
    }

    protected IDatabaseConnection getConnection() throws Exception {
        IDatabaseTester tester = getDatabaseTester();
        return tester.getConnection();
    }

    /**
     * Gets the IDatabaseTester for this testCase.<br>
     * Should this be public?
     */
    protected IDatabaseTester getDatabaseTester() {
        return this.databaseTester;
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
