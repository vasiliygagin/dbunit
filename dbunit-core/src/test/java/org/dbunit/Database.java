/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit;

import java.sql.Connection;

import org.dbunit.database.DatabaseConnection;

/**
 *
 */
public class Database {

    private Connection jdbcConnection;
    private DatabaseConnection connection;
    private JdbcDatabaseTester databaseTester;

    public Connection getJdbcConnection() {
        return jdbcConnection;
    }

    public void setJdbcConnection(Connection jdbcConnection) {
        this.jdbcConnection = jdbcConnection;
    }

    public DatabaseConnection getConnection() {
        return connection;
    }

    public void setConnection(DatabaseConnection connection) {
        this.connection = connection;
    }

    public JdbcDatabaseTester getDatabaseTester() {
        return databaseTester;
    }

    public void setDatabaseTester(JdbcDatabaseTester databaseTester) {
        this.databaseTester = databaseTester;
    }

}
