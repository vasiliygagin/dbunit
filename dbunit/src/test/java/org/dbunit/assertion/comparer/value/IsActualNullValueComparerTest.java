package org.dbunit.assertion.comparer.value;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import org.dbunit.DatabaseUnitException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.datatype.DataType;
import org.junit.Test;

public class IsActualNullValueComparerTest
{
    private IsActualNullValueComparer sut = new IsActualNullValueComparer();

    @Test
    public void testIsExpected_ActualNull_True() throws DatabaseUnitException
    {
        final ITable expectedTable = null;
        final ITable actualTable = null;
        final int rowNum = 0;
        final String columnName = null;
        final DataType dataType = DataType.BIGINT;
        final Object expectedValue = null;
        final Object actualValue = null;

        final boolean actual = sut.isExpected(expectedTable, actualTable,
                rowNum, columnName, dataType, expectedValue, actualValue);
        assertThat("Actual null should have been true.", actual, equalTo(true));
    }

    @Test
    public void testIsExpected_ActualNotNull_False()
            throws DatabaseUnitException
    {
        final ITable expectedTable = null;
        final ITable actualTable = null;
        final int rowNum = 0;
        final String columnName = null;
        final DataType dataType = DataType.BIGINT;
        final Object expectedValue = null;
        final Object actualValue = "not null string";

        final boolean actual = sut.isExpected(expectedTable, actualTable,
                rowNum, columnName, dataType, expectedValue, actualValue);
        assertThat("Actual not null should have been false.", actual,
                equalTo(false));
    }

    @Test
    public void testMakeFailMessage() throws Exception
    {
        final String actual = sut.makeFailMessage();

        assertThat("Should have fail phrase.", actual, not(nullValue()));
    }
}
