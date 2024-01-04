/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.assertion;

public class JUnitFailureFactory implements FailureFactory {
    @Override
    public Error createFailure(String message, String expected, String actual) {
        // Return the junit.framework.ComparisonFailure object
        return new ComparisonFailure(message, expected, actual);
    }

    @Override
    public Error createFailure(String message) {
        // Return the junit.framework.AssertionFailedError object
        return new AssertionFailedError(message);
    }
}
