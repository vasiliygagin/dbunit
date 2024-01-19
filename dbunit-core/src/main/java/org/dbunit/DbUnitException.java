/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit;

/**
 *
 */
public class DbUnitException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public DbUnitException(String message, Throwable cause) {
        super(message, cause);
    }
}
