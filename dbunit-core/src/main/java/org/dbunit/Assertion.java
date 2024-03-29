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

package org.dbunit;

import java.util.Map;
import java.util.function.Predicate;

import org.dbunit.assertion.ColumnValueComparerSource;
import org.dbunit.assertion.DbUnitAssert;
import org.dbunit.assertion.DbUnitValueComparerAssert;
import org.dbunit.assertion.DefaultFailureHandler;
import org.dbunit.assertion.FailureHandler;
import org.dbunit.assertion.MessageBuilder;
import org.dbunit.assertion.TableColumnValueComparerSource;
import org.dbunit.assertion.comparer.value.ValueComparer;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;

/**
 * Provides static methods for the most common DbUnit assertion needs.
 *
 * Although the methods are static, they rely on a {@link DbUnitAssert} instance
 * to do the work. So, if you need to customize this class behavior, you can
 * create your own {@link DbUnitAssert} extension.
 *
 * @author Manuel Laflamme
 * @author Felipe Leme (dbunit@felipeal.net)
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since 1.3 (Mar 22, 2002)
 */
public class Assertion {

    /** Assert using equals comparisons. */
    private static final DbUnitAssert EQUALS_INSTANCE = new DbUnitAssert();

    /** Assert using compare comparisons. @since 2.6.0 */
    private static final DbUnitValueComparerAssert VALUE_COMPARE_INSTANCE = new DbUnitValueComparerAssert();

    private Assertion() {
        throw new UnsupportedOperationException("this class has only static methods");
    }

    /**
     * @see DbUnitAssert#assertEqualsIgnoreCols(ITable, ITable, String[])
     */
    public static void assertEqualsIgnoreCols(final ITable expectedTable, final ITable actualTable,
            final String[] ignoreCols) throws DatabaseUnitException {
        EQUALS_INSTANCE.assertEqualsIgnoreCols(expectedTable, actualTable, ignoreCols);
    }

    /**
     * @see DbUnitAssert#assertEquals(IDataSet, IDataSet)
     */
    public static void assertEquals(final IDataSet expectedDataSet, final IDataSet actualDataSet)
            throws DatabaseUnitException {
        EQUALS_INSTANCE.assertEquals(expectedDataSet, actualDataSet);
    }

    /**
     * @see DbUnitAssert#assertEquals(IDataSet, IDataSet, FailureHandler)
     * @since 2.4
     */
    public static void assertEquals(final IDataSet expectedDataSet, final IDataSet actualDataSet,
            final FailureHandler failureHandler) throws DatabaseUnitException {
        EQUALS_INSTANCE.assertWithValueComparer(expectedDataSet, actualDataSet, failureHandler, null);
    }

    /**
     * @see DbUnitAssert#assertEquals(ITable, ITable)
     */
    public static void assertEquals(final ITable expectedTable, final ITable actualTable) throws DatabaseUnitException {
        EQUALS_INSTANCE.assertEquals(expectedTable, actualTable, (Predicate<Column>) c -> false);
    }

    /**
     * @see DbUnitAssert#assertEquals(ITable, ITable, Column[])
     */
    public static void assertEquals(final ITable expectedTable, final ITable actualTable,
            final Column[] additionalColumnInfo) throws DatabaseUnitException {
        EQUALS_INSTANCE.assertEquals(expectedTable, actualTable, additionalColumnInfo, (Predicate<Column>) c -> false);
    }

    /**
     * @see DbUnitAssert#assertEquals(ITable, ITable, FailureHandler)
     * @since 2.4
     */
    public static void assertEquals(final ITable expectedTable, final ITable actualTable,
            final FailureHandler failureHandler) throws DatabaseUnitException {
        EQUALS_INSTANCE.assertEquals(expectedTable, actualTable, failureHandler, c -> false);
    }

    /**
     * @see DbUnitValueComparerAssert#assertWithValueComparer(IDataSet, IDataSet,
     *      ValueComparer, Map)
     * @since 2.6.0
     */
    public static void assertWithValueComparer(final IDataSet expectedDataSet, final IDataSet actualDataSet,
            TableColumnValueComparerSource tableColumnValueComparerSource)
            throws DataSetException, Error, DatabaseUnitException {
        VALUE_COMPARE_INSTANCE.assertWithValueComparer(expectedDataSet, actualDataSet, tableColumnValueComparerSource);
    }

    /**
     * @see DbUnitValueComparerAssert#assertWithValueComparer(ITable, ITable,
     *      ValueComparer, Map)
     * @since 2.6.0
     */
    public static void assertWithValueComparer(final ITable expectedTable, final ITable actualTable,
            ColumnValueComparerSource columnValueComparerSource) throws DatabaseUnitException {
        VALUE_COMPARE_INSTANCE.assertWithValueComparer(expectedTable, actualTable, columnValueComparerSource);
    }

    /**
     * @see DbUnitValueComparerAssert#assertWithValueComparer(IDataSet, IDataSet,
     *      FailureHandler, ValueComparer, Map)
     * @since 2.6.0
     */
    public static void assertWithValueComparer(final IDataSet expectedDataSet, final IDataSet actualDataSet,
            final FailureHandler failureHandler, TableColumnValueComparerSource tableColumnValueComparerSource)
            throws DataSetException, Error, DatabaseUnitException {
        VALUE_COMPARE_INSTANCE.assertWithValueComparer(expectedDataSet, actualDataSet, failureHandler,
                tableColumnValueComparerSource);
    }

    /**
     * @see DbUnitValueComparerAssert#assertWithValueComparer(ITable, ITable,
     *      Column[], ValueComparer, Map)
     * @since 2.6.0
     */
    public static void assertWithValueComparer(final ITable expectedTable, final ITable actualTable,
            final Column[] additionalColumnInfo, ColumnValueComparerSource columnValueComparerSource)
            throws DatabaseUnitException {
        VALUE_COMPARE_INSTANCE.assertWithValueComparer(expectedTable, actualTable, additionalColumnInfo,
                columnValueComparerSource);
    }

    /**
     * @param excludedColumn TODO
     * @see DbUnitValueComparerAssert#assertWithValueComparer(ITable, ITable,
     *      FailureHandler, ValueComparer, Map, Predicate)
     * @since 2.6.0
     */
    public static void assertWithValueComparer(final ITable expectedTable, final ITable actualTable,
            final FailureHandler failureHandler, ColumnValueComparerSource columnValueComparerSource,
            Predicate<Column> excludedColumn) throws Error, DataSetException, DatabaseUnitException {
        MessageBuilder messageBuilder;
        if (failureHandler instanceof DefaultFailureHandler) {
            messageBuilder = ((DefaultFailureHandler) failureHandler).getMessageBuilder();
        } else {
            messageBuilder = new MessageBuilder(null);
        }

        VALUE_COMPARE_INSTANCE.assertWithValueComparerWithTableDefaults(expectedTable, actualTable, failureHandler,
                excludedColumn, messageBuilder, columnValueComparerSource,
                VALUE_COMPARE_INSTANCE.valueComparerDefaults);
    }

    public static DbUnitAssert getEqualsInstance() {
        return EQUALS_INSTANCE;
    }

    public static DbUnitValueComparerAssert getValueCompareInstance() {
        return VALUE_COMPARE_INSTANCE;
    }
}
