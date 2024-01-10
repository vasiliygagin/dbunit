/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.assertion;

public interface FailureHandler {

    /**
     * Creates a new failure object which can have different types, depending on the
     * testing framework you are currently using (e.g. JUnit, TestNG, ...)
     *
     * @param message  The reason for the failure
     * @param expected The expected result
     * @param actual   The actual result
     * @return The comparison failure object for this handler (can be JUnit or some
     *         other) which can be thrown on an assertion failure
     */
    void handleFailure(String message, String expected, String actual);

    /**
     * @param message The reason for the failure
     * @return The assertion failure object for this handler (can be JUnit or some
     *         other) which can be thrown on an assertion failure
     */
    void handleFailure(String message);
}
