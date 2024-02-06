/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit;

import org.dbunit.database.AbstractDatabaseConnection;

/**
 *
 */
public interface ConnectionSource {

    AbstractDatabaseConnection getConnection() throws DatabaseException;

    void releaseConnection(AbstractDatabaseConnection connection);
}
