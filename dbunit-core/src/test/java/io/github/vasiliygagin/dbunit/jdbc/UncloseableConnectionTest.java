/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package io.github.vasiliygagin.dbunit.jdbc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.sql.Connection;

import org.junit.Test;

public class UncloseableConnectionTest {

    private Connection connection = mock(Connection.class);
    private UncloseableConnection tested = new UncloseableConnection(connection);

    @Test
    public void doNotCloseConnection() throws Exception {
        tested.close();
        verifyNoInteractions(connection);
    }

    // rest of methods are generated delegates. Too lazy to test
}
