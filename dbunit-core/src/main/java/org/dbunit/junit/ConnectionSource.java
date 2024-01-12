/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit;

import org.dbunit.database.DatabaseConnection;

/**
 *
 */
public interface ConnectionSource {

    DatabaseConnection getConnection() throws DatabaseException;

    void releaseConnection(DatabaseConnection connection);
}
