/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation configures database connection to be used with given dbUnit test.
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface PropertiesDriverManagerConnection {

    /** A key for property that defines the connection url */
    public static final String DBUNIT_CONNECTION_URL = "dbunit.connectionUrl";
    /** A key for property that defines the driver classname */
    public static final String DBUNIT_DRIVER_CLASS = "dbunit.driverClass";
    /** A key for property that defines the username */
    public static final String DBUNIT_USERNAME = "dbunit.username";
    /** A key for property that defines the user's password */
    public static final String DBUNIT_PASSWORD = "dbunit.password";

    /** A key for property that defines the database schema */
    public static final String DBUNIT_SCHEMA = "dbunit.schema";
}
