/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal.connections;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.dbunit.junit.ConnectionSource;
import org.dbunit.junit.DatabaseException;
import org.dbunit.junit4.DdlExecutor;

/**
 * Manages multiple connections to multiple databases.
 * Work in progress.
 */
public class DatabaseConnectionManager {

    private Map<ConnectionKey, ConnectionSource> registeredConnectionSources = new HashMap<>();

    public ConnectionSource registerDataSourceInstance(DataSource dataSource) {
        DataSourceInstanceKey key = new DataSourceInstanceKey(dataSource);
        return registerDataSourceConnection(key, dataSource);
    }

    /**
     * @param dataSource
     * @return
     */
    public ConnectionSource registerDataSourceByType(DataSource dataSource) {
        DataSourceClassKey key = new DataSourceClassKey(dataSource.getClass());
        return registerDataSourceConnection(key, dataSource);
    }

    private ConnectionSource registerDataSourceConnection(ConnectionKey key, DataSource dataSource) {
        ConnectionSource connectionSource = registeredConnectionSources.get(key);
        if (connectionSource == null) {
            connectionSource = new DataSourceConnectionSource(dataSource);
//            initDB(connectionSource);
            registeredConnectionSources.put(key, connectionSource);
        }
        return connectionSource;
    }

    private void initDB(ConnectionSource connectionSource) {
        try {
            Connection jdbcConnection = connectionSource.getConnection().getConnection();
            final File ddlFile = new File("src/test/resources/sql/hypersonic.sql");
            DdlExecutor.executeDdl(jdbcConnection, ddlFile);
        } catch (IOException | SQLException | DatabaseException exc) {
            throw new AssertionError("Unsble to initialize database", exc);
        }
    }
}
