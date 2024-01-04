/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.assertion.comparer.value;

class AssertNotNull {

    public static void assertNotNull(String message, Object object) {
        if (object == null) {
            if (message == null) {
                throw new AssertionError();
            }
            throw new AssertionError(message);
        }
    }
}
