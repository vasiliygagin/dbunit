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
import org.dbunit.junit.DbUnitFacade;
import org.dbunit.operation.DatabaseOperation;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

/**
 * Convenience class for writing JUnit tests with dbunit easily. <br />
 * Note that there are some even more convenient classes available such as
 * {@link DBTestCase}.
 */
public abstract class DatabaseTestCase2 {

    @Rule
    public final DbUnitFacade dbUnit = new DbUnitFacade();

    private DefaultDatabaseTester databaseTester;

    public DatabaseTestCase2() {
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
