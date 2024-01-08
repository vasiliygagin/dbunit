/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.internal.connections;

import static org.junit.Assert.assertEquals;
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

    @Test
    public void equalsIfWrappedConnectionsEqual() {
        assertEquals(tested, new UncloseableConnection(connection));
    }

    @Test
    public void hashCodeFromWrappedConnection() {
        assertEquals(connection.hashCode(), tested.hashCode());
    }
}
