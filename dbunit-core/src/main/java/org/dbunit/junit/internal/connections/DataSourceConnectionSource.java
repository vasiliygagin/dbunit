/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal.connections;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.metadata.MetadataManager;
import org.dbunit.junit.ConnectionSource;
import org.dbunit.junit.DatabaseException;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 *
 */
public class DataSourceConnectionSource implements ConnectionSource {

    private final DataSource dataSource;

    public DataSourceConnectionSource(DataSource dataSource) {
        this.dataSource = dataSource;
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
            DatabaseConfig config = new DatabaseConfig();
            MetadataManager metadataManager = new MetadataManager(jdbcConnection, config, null, null);
            return new DatabaseConnection(jdbcConnection, config, "PUBLIC", metadataManager);
        } catch (DatabaseUnitException exc) {
            throw new DatabaseException(exc);
        }
    }

    @Override
    public void releaseConnection(DatabaseConnection connection) {
        try {
            connection.getConnection().close();
        } catch (SQLException exc) {
            exc.printStackTrace();
        }
    }
}
