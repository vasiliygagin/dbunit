/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.internal.connections;

import java.sql.Connection;

/**
 *
 */
public interface DriverManagerConnectionSource {

    Connection fetchConnection(String driver, String url, String user, String password);
}
