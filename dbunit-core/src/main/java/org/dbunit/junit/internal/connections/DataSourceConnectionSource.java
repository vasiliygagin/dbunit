/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal.connections;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.AbstractDatabaseConnection;
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
    private final DatabaseConfig config;
    private final MetadataManager metadataManager;

    public DataSourceConnectionSource(DataSource dataSource) throws DatabaseException {
        this.dataSource = dataSource;
        this.config = new DatabaseConfig();

        try ( //
                Connection jdbcConnection = dataSource.getConnection(); //
        ) {
            this.metadataManager = new MetadataManager(jdbcConnection, config, null, null);
        } catch (SQLException exc) {
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
            return new DatabaseConnection(jdbcConnection, config, "PUBLIC", metadataManager);
        } catch (DatabaseUnitException exc) {
            throw new DatabaseException(exc);
        }
    }

    @Override
    public void releaseConnection(AbstractDatabaseConnection connection) {
        try {
            connection.getConnection().close();
        } catch (SQLException exc) {
            exc.printStackTrace();
        }
    }
}
