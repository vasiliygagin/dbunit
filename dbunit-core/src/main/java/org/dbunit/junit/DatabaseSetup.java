/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit;

/**
 *
 */
public @interface DatabaseSetup {

    /**
     * @return  SQL script file name to run before test case, to setup tables and data
     */
    String sqlBefore() default "";

    /**
     * @return  SQL script file name to run after test case, to tear down tables and data setup in {@link #sqlBefore()}
     */
    String sqlAfter() default "";
}
