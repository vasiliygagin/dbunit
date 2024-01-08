/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
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
@Repeatable(DriverManagerConnections.class)
public @interface DriverManagerConnection {

    /**
     * @return Name, to register dataSource under.
     */
    String name() default "";

    /**
     * @return JDBC driver class name
     */
    String driver();

    /**
     * @return database url in format understood by {@link java.sql.DriverManager#getConnection(String, String, String)}
     */
    String url();

    /**
     * @return user name for database connection
     */
    String user() default "";

    /**
     * @return user password for database connection
     */
    String password() default "";
}
