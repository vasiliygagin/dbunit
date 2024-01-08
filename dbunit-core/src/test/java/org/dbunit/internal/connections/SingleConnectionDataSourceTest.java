/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.internal.connections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.sql.Connection;

import org.junit.Test;

public class SingleConnectionDataSourceTest {

    private Connection connection = mock(Connection.class);
    private SingleConnectionDataSource tested = new SingleConnectionDataSource(connection);

    @Test(expected = IllegalArgumentException.class)
    public void ensureConnectionIsThere() {
        new SingleConnectionDataSource(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getLogWriter() throws Exception {
        tested.getLogWriter();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setLogWriter() throws Exception {
        tested.setLogWriter(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setLoginTimeout() throws Exception {
        tested.setLoginTimeout(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getLoginTimeout() throws Exception {
        tested.getLoginTimeout();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getParentLogger() throws Exception {
        tested.getParentLogger();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void unwrap() throws Exception {
        tested.unwrap(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void isWrapperFor() throws Exception {
        tested.isWrapperFor(null);
    }

    @Test
    public void getConnection() throws Exception {
        Connection result = tested.getConnection();
        assertEquals(connection, ((UncloseableConnection) result).delegate);
    }

    @Test
    public void getConnectionWithcredentials() throws Exception {
        Connection result = tested.getConnection(null, null);
        assertEquals(connection, ((UncloseableConnection) result).delegate);
    }

    @Test
    public void equalsIfWrappedConnectionsEqual() {
        assertEquals(tested, new SingleConnectionDataSource(connection));
    }

    @Test
    public void hashCodeFromWrappedConnection() {
        assertEquals(connection.hashCode(), tested.hashCode());
    }
}
