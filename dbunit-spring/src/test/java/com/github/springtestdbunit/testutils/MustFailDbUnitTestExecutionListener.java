/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package com.github.springtestdbunit.testutils;

import static org.junit.Assert.fail;

import org.springframework.test.context.TestContext;

import com.github.springtestdbunit.DbUnitTestExecutionListener;

public class MustFailDbUnitTestExecutionListener extends DbUnitTestExecutionListener {

    @Override
    public void afterTestMethod(TestContext testContext) throws Exception {
        try {
            super.afterTestMethod(testContext);
            fail("Test did not fail");
        } catch (Throwable ex) {
        }
    }
}
