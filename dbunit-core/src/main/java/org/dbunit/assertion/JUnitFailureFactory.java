/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.assertion;

public class JUnitFailureFactory implements FailureHandler {

    @Override
    public void handleFailure(String message, String expected, String actual) {
        // Return the junit.framework.ComparisonFailure object
        throw new ComparisonFailure(message, expected, actual);
    }

    @Override
    public void handleFailure(String message) {
        // Return the junit.framework.AssertionFailedError object
        throw new AssertionFailedError(message);
    }
}
