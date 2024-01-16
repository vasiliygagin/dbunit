/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit4;

import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.junit.DbUnitFacade;
import org.dbunit.operation.DatabaseOperation;
import org.junit.Before;
import org.junit.Rule;

/**
 * Convenience class for writing JUnit tests with dbunit easily. <br />
 * Note that there are some even more convenient classes available such as
 * {@link DBTestCase}.
 */
public abstract class DatabaseTestCase {

    @Rule
    public final DbUnitFacade dbUnit = new DbUnitFacade();

    /**
     * Returns the test dataset.
     */
    protected IDataSet getDataSet() throws Exception {
        return new DefaultDataSet();
    }

    @Before
    public final void setUpDatabaseTester() throws Exception {
        dbUnit.executeOperation(DatabaseOperation.CLEAN_INSERT, getDataSet());
    }
}
