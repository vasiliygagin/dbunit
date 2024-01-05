/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package io.github.vasiliygagin.dbunit.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;

/**
 * {@link DataSource}, which always return same connection.
 * Only provides implementations fro methods required by DbUnit.
 * NOTE: Connection state is not managed yet. So do not close connection.
 */
public class SingleConnectionDataSource implements DataSource {

    private final Connection connection;

    public SingleConnectionDataSource(Connection connection) {
        if (connection == null) {
            throw new IllegalArgumentException("The parameter 'connection' must not be null");
        }
        this.connection = new UncloseableConnection(connection);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return connection;
    }
}
