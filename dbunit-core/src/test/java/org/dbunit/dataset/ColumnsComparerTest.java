/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.dataset;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.dbunit.assertion.FailureHandler;
import org.dbunit.dataset.datatype.DataType;
import org.junit.Test;

/**
 *
 */
public class ColumnsComparerTest {

    private ColumnsComparer tested = new ColumnsComparer();

    @Test
    public void testGetColumnDiff_NoDifference() throws Exception {
        Column[] expectedColumns = { new Column("c0", DataType.UNKNOWN), new Column("c1", DataType.UNKNOWN), };
        Column[] actualColumns = { new Column("c0", DataType.UNKNOWN), new Column("c1", DataType.UNKNOWN), };
        ITableMetaData metaDataExpected = createMetaData(expectedColumns);
        ITableMetaData metaDataActual = createMetaData(actualColumns);
        FailureHandler failureHandler = mock(FailureHandler.class);

        tested.compareColumns(metaDataExpected, metaDataActual, c -> false, failureHandler);

        verifyNoInteractions(failureHandler);
    }

    @Test
    public void testGetColumnDiffDifferentOrder_NoDifference() throws Exception {
        // order [c0, c1]
        Column[] expectedColumns = { new Column("c0", DataType.UNKNOWN), new Column("c1", DataType.UNKNOWN), };
        // order [c1, c0]
        Column[] actualColumnsDifferentOrder = { new Column("c1", DataType.UNKNOWN),
                new Column("c0", DataType.UNKNOWN), };
        ITableMetaData metaDataExpected = createMetaData(expectedColumns);
        ITableMetaData metaDataActual = createMetaData(actualColumnsDifferentOrder);
        FailureHandler failureHandler = mock(FailureHandler.class);

        tested.compareColumns(metaDataExpected, metaDataActual, c -> false, failureHandler);

        verifyNoInteractions(failureHandler);
    }

    @Test
    public void testGetColumnDiff_Difference() throws Exception {
        Column[] expectedColumns = { new Column("c0", DataType.UNKNOWN), new Column("c2", DataType.UNKNOWN),
                new Column("c1", DataType.UNKNOWN), };
        Column[] actualColumns = { new Column("d0", DataType.UNKNOWN), new Column("c2", DataType.UNKNOWN), };
        ITableMetaData metaDataExpected = createMetaData(expectedColumns);
        ITableMetaData metaDataActual = createMetaData(actualColumns);

        // Create the difference
        FailureHandler failureHandler = mock(FailureHandler.class);

        tested.compareColumns(metaDataExpected, metaDataActual, c -> false, failureHandler);

        verify(failureHandler).handleFailure("column count (table=MY_TABLE, expectedColCount=3, actualColCount=2)",
                "[c0, c2, c1]", "[d0, c2]");
    }

    private ITableMetaData createMetaData(Column[] columns) {
        return new DefaultTableMetaData("MY_TABLE", columns);
    }
}
