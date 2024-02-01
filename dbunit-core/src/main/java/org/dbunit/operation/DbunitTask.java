/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.operation;

import org.dbunit.junit.internal.TestContext;

/**
 * Thing dbUnit can run later
 */
@FunctionalInterface
public interface DbunitTask {

    void execute(TestContext testContext) throws Exception;
}
