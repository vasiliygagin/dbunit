/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package com.github.springtestdbunit.testutils;

import org.junit.Assert;
import org.springframework.test.context.TestContext;

import com.github.springtestdbunit.DbUnitTestExecutionListener;

public class MustNoSwallowTestExecutionListener extends DbUnitTestExecutionListener {

    @Override
    public void afterTestMethod(TestContext testContext) throws Exception {
        Assert.assertTrue(testContext.getTestException() instanceof NotSwallowedException);
        super.afterTestMethod(testContext);
    }

}
