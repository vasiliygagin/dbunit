/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit;

import org.dbunit.junit.internal.TestContext;
import org.dbunit.junit4.DbunitTestCaseTestRunner;

/**
 *
 */
public class DbUnitTestFacade extends DbUnitFacade {

    @Override
    protected void after() {
        // Need to delay releaseConnections() so we can do assertAfter() in internal tests
        rollbackConnections();
//        super.after();
        TestContext testContext = getTestContext();
        DbunitTestCaseTestRunner.assertAfter(() -> testContext.releaseConnections());
    }

}
