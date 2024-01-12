/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal.connections;

import java.util.Objects;

import javax.sql.DataSource;

/**
 *
 */
class DataSourceClassKey implements ConnectionKey {

    private final Class<? extends DataSource> klass;

    public DataSourceClassKey(Class<? extends DataSource> klass) {
        this.klass = klass;
    }

    @Override
    public int hashCode() {
        return Objects.hash(klass);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DataSourceClassKey other = (DataSourceClassKey) obj;
        return Objects.equals(klass, other.klass);
    }
}
