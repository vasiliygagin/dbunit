/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit;

/**
 *
 */
public class DatabaseException extends Exception {

    private static final long serialVersionUID = 1L;

    public DatabaseException(String message) {
        super(message);
    }

    /**
     * @param exc
     */
    public DatabaseException(Exception exc) {
        super(exc);
    }
}
