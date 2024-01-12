/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.internal.connections;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class DriverManagerConnectionsFactory {

    private static DriverManagerConnectionsFactory IT = new DriverManagerConnectionsFactory();
    {
        IT = this;
    }

    private DriverManagerConnectionsFactory() {
    }

    public static DriverManagerConnectionsFactory getIT() {
        return IT;
    }

    private Map<ConnectionKey, Connection> registeredConnections = new HashMap<>();

    public Connection fetchConnection(String driver, String url, String user, String password) {
        ConnectionKey key = new ConnectionKey(url, user);
        Connection connection = registeredConnections.get(key);
        if (connection == null) {
            connection = buildConnection(driver, url, user, password);
            connection = new UncloseableConnection(connection);
            registeredConnections.put(key, connection);
        }

        return connection;
    }

    public Connection buildConnection(String driver, String url, String user, String password) {
        Connection connection;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, user, password);
            connection.setAutoCommit(false);
        } catch (ClassNotFoundException | SQLException exc) {
            throw new AssertionError(" Unable to connect to [" + url + "]", exc);
        }
        return connection;
    }

    private static class ConnectionKey {

        private final String url;
        private final String user;

        public ConnectionKey(String url, String user) {
            this.url = url;
            this.user = user;
        }

        @Override
        public int hashCode() {
            return Objects.hash(url, user);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ConnectionKey other = (ConnectionKey) obj;
            return Objects.equals(url, other.url) && Objects.equals(user, other.user);
        }
    }
}
