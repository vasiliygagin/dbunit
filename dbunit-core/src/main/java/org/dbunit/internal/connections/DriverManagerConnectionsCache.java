/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.internal.connections;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class DriverManagerConnectionsCache implements DriverManagerConnectionSource {

    private final DriverManagerConnectionsFactory source;

    public DriverManagerConnectionsCache(DriverManagerConnectionsFactory source) {
        this.source = source;
    }

    private Map<ConnectionKey, Connection> registeredConnections = new HashMap<>();

    @Override
    public Connection fetchConnection(String driver, String url, String user, String password) {
        ConnectionKey key = new ConnectionKey(url, user);
        Connection connection = registeredConnections.get(key);
        if (connection == null) {
            connection = source.fetchConnection(driver, url, user, password);
            connection = new UncloseableConnection(connection);
            registeredConnections.put(key, connection);
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
