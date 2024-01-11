/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.assertion;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link FailureHandler} that collects failures without throwing them.
 */
public class DiffCollectingFailureHandler extends DefaultFailureHandler {

    private final List<DbComparisonFailure> errors = new ArrayList<>();

    @Override
    public void handleFailure(String message, String expected, String actual) {
        DbComparisonFailure error = new DbComparisonFailure(message, expected, actual);
        error.fillInStackTrace();
        errors.add(error);
    }

    /**
     * @return The list of collected {@link AssertionError}s
     */
    public List<DbComparisonFailure> getErrors() {
        return errors;
    }
}
