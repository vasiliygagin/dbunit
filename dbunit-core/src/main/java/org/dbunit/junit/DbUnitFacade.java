/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit;

import java.sql.Connection;
import java.sql.SQLException;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.junit.internal.DbUnitRule;
import org.dbunit.operation.DatabaseOperation;

/**
 * JUnit 4 way of integrating DbUnit framework into JUnit
 *
 * <pre>
 * </pre>
 */
public class DbUnitFacade extends DbUnitRule {

    /**
     * @return a connection to default database
     * @throws DatabaseException
     */
    public DatabaseConnection getConnection() throws DatabaseException {
        return getTestContext().getConnection();
    }

    public Connection getJdbcConnection() throws Exception {
        return getConnection().getConnection();
    }

    public void executeOperation(DatabaseOperation operation, IDataSet dataSet)
            throws DatabaseUnitException, SQLException, DatabaseException {
        operation.execute(getConnection(), dataSet);
    }
}
