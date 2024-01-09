/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit;

import static org.junit.Assume.assumeTrue;

import org.junit.After;
import org.junit.Before;

public class AbstractDatabaseTest {

    protected final DatabaseEnvironment environment;
    protected Database database;
    private boolean environmentOk;

    public AbstractDatabaseTest() throws Exception {
        environment = DatabaseEnvironmentLoader.getInstance();
    }

    @Before
    public final void openDatabase() throws Exception {
        environmentOk = checkEnvironment();
        assumeTrue(environmentOk);
        database = environment.openDatabase(".");
    }

    /**
     * @return <code>true</code> if environment is ok to run tests. Which is default. Otherwise tests are ignored
     */
    protected boolean checkEnvironment() {
        return true;
    }

    /**
     * If environment is not Ok, then Befores did not run. And presumably Afters do not need to run either. Though JUnit is running them.
     * This method gives you an opportunity to check inside After if it should be executed or not.
     * @return
     */
    public boolean isEnvironmentOk() {
        return environmentOk;
    }

    @After
    public final void closeDatabase() {
        if (environmentOk) {
            environment.closeDatabase(database);
        }
    }
}
