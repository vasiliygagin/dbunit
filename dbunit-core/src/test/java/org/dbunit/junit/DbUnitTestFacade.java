/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit;

import org.dbunit.junit4.DbunitTestCaseTestRunner;

/**
 *
 */
public class DbUnitTestFacade extends DbUnitFacade {

    // Need to delay releaseTestContext() so we can do assertAfter() in internal tests
    @Override
    protected void releaseTestContext() {
        // Need to delay releaseTestContext() so we can do assertAfter() in internal tests
        // So creating fake assert to run last
        DbunitTestCaseTestRunner.assertAfter(() -> {
            super.releaseTestContext();
        });
    }
}
