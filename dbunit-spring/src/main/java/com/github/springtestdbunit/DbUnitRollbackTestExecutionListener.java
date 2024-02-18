/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package com.github.springtestdbunit;

import org.dbunit.junit.internal.TestContextAccessor;
import org.dbunit.junit.internal.TestContextDriver;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.AbstractTestExecutionListener;

public class DbUnitRollbackTestExecutionListener extends AbstractTestExecutionListener {

    private TestContextDriver testContextDriver;

    @Override
    public void prepareTestInstance(TestContext testContext) {
        testContextDriver = TestContextAccessor.buildTestContext();
    }

    @Override
    public void afterTestMethod(TestContext testContext) {
        testContextDriver.rollbackConnections();
        testContextDriver.releaseTestContext();
    }
}
