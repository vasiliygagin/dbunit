/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal.connections;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.junit.ConnectionSource;
import org.dbunit.junit.DatabaseException;
import org.dbunit.junit4.DdlExecutor;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 *
 */
public class DataSourceConnectionSource implements ConnectionSource {

    private final DataSource dataSource;

    public DataSourceConnectionSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private void initDB() throws DatabaseException {
        try (//
                Connection jdbcConnection = dataSource.getConnection(); //
        ) {
            final File ddlFile = new File("src/test/resources/sql/hypersonic.sql");
            DdlExecutor.executeDdl(jdbcConnection, ddlFile);
        } catch (SQLException | IOException exc) {
            throw new DatabaseException(exc);
        }
    }

    @Override
    public DatabaseConnection getConnection() throws DatabaseException {
        try {
            return buildDatabaseConnection(dataSource.getConnection());
        } catch (DatabaseException exc) {
            throw exc;
        } catch (SQLException exc) {
            throw new DatabaseException(exc);
        }
    }

    private DatabaseConnection buildDatabaseConnection(Connection jdbcConnection) throws DatabaseException {
        try {
            return new DatabaseConnection(jdbcConnection, new DatabaseConfig(), "PUBLIC");
        } catch (DatabaseUnitException exc) {
            throw new DatabaseException(exc);
        }
    }

    @Override
    public void releaseConnection(DatabaseConnection connection) {
        try {
            Connection jdbcConnection = connection.getConnection();
            jdbcConnection.rollback();
            jdbcConnection.close();
        } catch (SQLException exc) {
            exc.printStackTrace();
        }
    }
}
