/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal.connections;

import java.sql.DriverManager;
import java.util.Objects;

/**
 * Represents connection information for connections made via {@link DriverManager}
 */
class DriverManagerConnectionKey implements ConnectionKey {

    private final String driver;
    private final String url;
    private final String user;
    private final String password;

    public DriverManagerConnectionKey(String driver, String url, String user, String password) {
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;
    }

    @Override
    public int hashCode() {
        return Objects.hash(driver, password, url, user);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DriverManagerConnectionKey other = (DriverManagerConnectionKey) obj;
        return Objects.equals(driver, other.driver) && Objects.equals(password, other.password)
                && Objects.equals(url, other.url) && Objects.equals(user, other.user);
    }
}
