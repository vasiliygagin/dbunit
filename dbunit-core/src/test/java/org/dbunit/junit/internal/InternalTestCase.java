/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal;

/**
 * Used with {@link DbunitTestCaseTestRunner}. If test case implements this interface, runner will call callback before and after test execution
 */
public interface InternalTestCase {

    /**
     * Called before test execution
     */
    default void beforeTestCase() throws Exception {
    }

    /**
     * Called after test execution
     */
    default void afterTestCase() throws Exception {
    }
}
