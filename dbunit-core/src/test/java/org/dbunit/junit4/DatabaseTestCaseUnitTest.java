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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;

import org.dbunit.dataset.IDataSet;
import org.dbunit.junit.DbUnitFacade;
import org.dbunit.operation.DatabaseOperation;
import org.junit.Test;

/**
 * @author Vasiliy Gagin
 */
public class DatabaseTestCaseUnitTest {

    private DatabaseTestCase tested = spy(DatabaseTestCase.class);

    @Test
    public void initializesWithEmptyDataSet() throws Exception {
        IDataSet dataSet = tested.getDataSet();
        assertEquals(0, dataSet.getTableNames().length);
    }

    @Test
    public void shouldCleanInsertOnSetup() throws Exception {
        DbUnitFacade facade = mock(DbUnitFacade.class);
        replaceFacade(facade);

        IDataSet dataSet = mock(IDataSet.class);
        doReturn(dataSet).when(tested).getDataSet();

        tested.setUpDatabaseTester();

        verify(facade).executeOperation(DatabaseOperation.CLEAN_INSERT, dataSet);
    }

    protected void replaceFacade(DbUnitFacade facade) throws NoSuchFieldException, IllegalAccessException {
        Field dbUnitField = DatabaseTestCase.class.getDeclaredField("dbUnit");
        dbUnitField.setAccessible(true);
        dbUnitField.set(tested, facade);
    }
}
