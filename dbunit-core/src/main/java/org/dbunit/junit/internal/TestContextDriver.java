/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal;

import java.lang.reflect.Method;

import org.dbunit.junit.DatabaseException;

/**
 *
 */
public interface TestContextDriver {

    TestContext getTestContext();

    void configureTestContext(Class<?> klass, Method method) throws DatabaseException;

    void beforeTest() throws Exception;

    void afterTest() throws Exception;

    void rollbackConnections();

    void releaseTestContext();
}
