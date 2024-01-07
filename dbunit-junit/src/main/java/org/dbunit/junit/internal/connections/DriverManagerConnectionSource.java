/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal.connections;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.junit.ConnectionSource;
import org.dbunit.junit.DatabaseException;
import org.dbunit.junit4.DdlExecutor;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 *
 */
public class DriverManagerConnectionSource implements ConnectionSource {

    private final DatabaseConnection connection;
    private final DatabaseException exception;

    public DriverManagerConnectionSource(String driver, String url, String user, String password) {
        DatabaseException exception = null;
        DatabaseConnection connection = null;
        try {
            connection = buildDatabaseConnection(buildJdbcConnection(driver, url, user, password));
            initDB(connection);
        } catch (DatabaseException exc) {
            exception = exc;
        } catch (Exception exc) {
            exception = new DatabaseException(exc);
        }
        this.connection = connection;
        this.exception = exception;

    }

    private void initDB(DatabaseConnection connection) throws DatabaseException {
        final File ddlFile = new File("src/test/resources/sql/hypersonic.sql");
        try {
            DdlExecutor.executeDdl(connection.getConnection(), ddlFile);
        } catch (Exception exc) {
            throw new DatabaseException(exc);
        }
    }

    @Override
    public DatabaseConnection getConnection() throws DatabaseException {
        if (exception != null) {
            throw exception;
        }
        return connection;
    }

    private DatabaseConnection buildDatabaseConnection(Connection jdbcConnection) throws DatabaseException {
        try {
            return new DatabaseConnection(jdbcConnection, new DatabaseConfig(), "PUBLIC");
        } catch (DatabaseUnitException exc) {
            throw new DatabaseException(exc);
        }
    }

    private Connection buildJdbcConnection(String driver, String url, String user, String password) throws Exception {
        Class.forName(driver);
        Connection connection = DriverManager.getConnection(url, user, password);
        /*
        Connection conn = null;
        if (username == null && password == null) {
            conn = DriverManager.getConnection(connectionUrl);
        } else {
            conn = DriverManager.getConnection(connectionUrl, username, password);
        }
         */
        connection.setAutoCommit(false);
        return connection;
    }

    @Override
    public void releaseConnection(@SuppressWarnings("unused") DatabaseConnection connection) {
        try {
            connection.getConnection().rollback();
        } catch (SQLException exc) {
            exc.printStackTrace();
        }
    }
}
