/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit4;

@FunctionalInterface
public interface DbunitAssert {

    void execute() throws Throwable;
}
