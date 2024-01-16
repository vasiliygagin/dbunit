/*
 *
 * The DbUnit Database Testing Framework
 * Copyright (C)2002-2004, DbUnit.org
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */
package org.dbunit.junit4;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;

import org.dbunit.dataset.IDataSet;
import org.dbunit.junit.DbUnitFacade;
import org.dbunit.junit.internal.DbunitTestCaseTestRunner;
import org.dbunit.junit.internal.InternalTestCase;
import org.dbunit.operation.DatabaseOperation;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * This test case will run similar to how users of {@link DatabaseTestCase} will use it.
 * Difference is  {@link DbunitTestCaseTestRunner} and {@link InternalTestCase}. Which are needed to give me opportunity to replace dbUnit rule with mock.
 * @author Vasiliy Gagin
 */
@RunWith(DbunitTestCaseTestRunner.class)
public class DatabaseTestCaseTest extends DatabaseTestCase implements InternalTestCase {

    private final DbUnitFacade dbUnit = mock(DbUnitFacade.class);
    private final IDataSet dataSet = mock(IDataSet.class);

    @Override
    public void beforeTestCase() throws Exception {
        Field dbUnitField = DatabaseTestCase.class.getDeclaredField("dbUnit");
        dbUnitField.setAccessible(true);
        dbUnitField.set(this, dbUnit);
    }

    @Override
    protected IDataSet getDataSet() throws Exception {
        return dataSet;
    }

    @Test
    public void cleanInsertIsExecutedOnBefore() throws Exception {
        verify(dbUnit).executeOperation(DatabaseOperation.CLEAN_INSERT, dataSet);
    }
}
