/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.internal.connections;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 */
public class DriverManagerConnectionsFactory implements DriverManagerConnectionSource {

    @Override
    public Connection fetchConnection(String driver, String url, String user, String password) {
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
}
