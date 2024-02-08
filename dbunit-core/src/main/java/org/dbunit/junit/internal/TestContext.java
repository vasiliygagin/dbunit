/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.dbunit.database.AbstractDatabaseConnection;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.junit.ConnectionSource;
import org.dbunit.junit.DatabaseException;
import org.dbunit.operation.DbunitTask;

/**
 *
 */
public class TestContext {

    private List<DbunitTask> tasksBefore = new ArrayList<>();
    private List<DbunitTask> tasksAfter = new ArrayList<>();

    private Map<String, ConnectionSource> connectionSources = new HashMap<>();
    private Map<String, AbstractDatabaseConnection> connections = new HashMap<>();
    private String schema;

    public AbstractDatabaseConnection getConnection() throws DatabaseException {
        String connectionName = getConnectionName();
        return getConnection(connectionName);
    }

    /**
     * @param connectionName
     * @return
     * @throws DatabaseException
     */
    public AbstractDatabaseConnection getConnection(String connectionName) throws DatabaseException {
        AbstractDatabaseConnection databaseConnection = connections.get(connectionName);
        if (databaseConnection == null) {
            ConnectionSource connectionSource = connectionSources.get(connectionName);
            if (connectionSource == null) {
                throw new AssertionError("No connection named [" + connectionName + "] is registered for test case");
            }
            databaseConnection = connectionSource.getConnection();
            connections.put(connectionName, databaseConnection);
        }
        return databaseConnection;
    }

    public String getConnectionName() {
        if (connectionSources.size() > 1) {
            throw new AssertionError("Multiple connections registered for test case");
        }
        if (connectionSources.size() == 1) {
            return connectionSources.keySet().iterator().next();
        }
        throw new AssertionError("No connections registered for test case");
    }

    /**
     * @param dataSourceName
     * @param connectionSource
     */
    public void addConnecionSource(String dataSourceName, ConnectionSource connectionSource) {
        if (connectionSources.containsKey(dataSourceName)) {
            throw new IllegalStateException("Datasource with name [" + dataSourceName + "] is already registered");
        }
        connectionSources.put(dataSourceName, connectionSource);

    }

    public ConnectionSource getSingleConnectionSource() {
        if (connectionSources.size() > 1) {
            throw new AssertionError("Multiple connections registered for test case");
        }
        if (connectionSources.size() == 1) {
            return connectionSources.values().iterator().next();
        }
        throw new AssertionError("No connections registered for test case");
    }

    public void releaseConnections() {
        for (Entry<String, AbstractDatabaseConnection> entry : connections.entrySet()) {
            String connectionName = entry.getKey();
            AbstractDatabaseConnection connection = entry.getValue();
            connectionSources.get(connectionName).releaseConnection(connection);
        }
        connections.clear();
        connectionSources.clear();
    }

    public void shutdownConnections() {
        for (AbstractDatabaseConnection connection : connections.values()) {
            ((DatabaseConnection) connection).shutdown();
        }
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public void addTaskBefore(DbunitTask task) {
        tasksBefore.add(task);
    }

    public void addTaskAfter(DbunitTask task) {
        tasksAfter.add(task);
    }

    private void runTasksBefore() throws Exception {
        for (DbunitTask task : tasksBefore) {
            task.execute(this);
        }
    }

    private void runTasksAfter() throws Exception {
        for (DbunitTask task : tasksAfter) {
            task.execute(this);
        }
    }

    void configureTestContext(Class<?> klass, Method method) throws DatabaseException {
        GlobalContext.getIt().getAnnotationProcessor().configureTest(klass, method, this);
    }

    void beforeTest() throws Exception {
        runTasksBefore();
    }

    void afterTest() throws Exception {
        runTasksAfter();
    }

    void rollbackConnections() {
        for (AbstractDatabaseConnection connection : connections.values()) {
            connection.rollback();
        }
    }
}
