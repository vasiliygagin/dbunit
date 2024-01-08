/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal.connections;

import java.util.Objects;

import javax.sql.DataSource;

/**
 *
 */
class DataSourceInstanceKey implements ConnectionKey {

    private final DataSource dataSource;

    public DataSourceInstanceKey(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataSource);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DataSourceInstanceKey other = (DataSourceInstanceKey) obj;
        return Objects.equals(dataSource, other.dataSource);
    }
}
