/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit;

import java.sql.Connection;

import org.dbunit.database.DatabaseConnection;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 *
 */
public class Database {

    public final DatabaseTestingEnvironment environment;
    public final DatabaseConfig databaseConfig;
    private Connection jdbcConnection;
    private DatabaseConnection connection;
    private JdbcDatabaseTester databaseTester;

    public Database(DatabaseTestingEnvironment environment, DatabaseConfig databaseConfig) {
        this.environment = environment;
        this.databaseConfig = databaseConfig;
    }

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
