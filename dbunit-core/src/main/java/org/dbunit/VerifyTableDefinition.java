/*
 *
 * The DbUnit Database Testing Framework
 * Copyright (C)2002-2008, DbUnit.org
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

import java.util.Arrays;
import java.util.Map;

import org.dbunit.assertion.ColumnValueComparerSource;
import org.dbunit.assertion.comparer.value.ValueComparer;

/**
 * Defines a database table to verify (assert on data), specifying include and
 * exclude column filters and optional {@link ValueComparer}s.
 *
 * @author Jeff Jensen jeffjensen AT users.sourceforge.net
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since 2.4.8
 */
public class VerifyTableDefinition {

    /** The name of the table. */
    private final String tableName;

    /** The columns to exclude in table comparisons. */
    private final String[] columnExclusionFilters;

    /** The columns to include in table comparisons. */
    private final String[] columnInclusionFilters;

    /**
     * {@link ValueComparer} to use with column value comparisons when the column
     * name for the table is not in the {@link columnValueComparers} {@link Map}.
     * Can be <code>null</code> and will default.
     * Map of column names to {@link ValueComparer}s to use for comparison.
     *
     * @since 2.6.0
     */
    private final ColumnValueComparerSource columnValueComparerSource;

    /**
     * Create a valid instance with all columns compared except exclude the
     * specified columns.
     *
     * @param table          The name of the table - required.
     * @param excludeColumns The columns in the table to ignore (filter out) in
     *                       expected vs actual comparisons; null or empty array to
     *                       exclude no columns.
     */
    public VerifyTableDefinition(final String table, final String[] excludeColumns) {
        this(table, excludeColumns, null, null);
    }

    /**
     * Create a valid instance with all columns compared and use the specified
     * defaultValueComparer for all column comparisons not in the
     * columnValueComparers {@link Map}.
     *
     * @param table                The name of the table - required.
     * @param defaultValueComparer {@link ValueComparer} to use with column value
     *                             comparisons when the column name for the table is
     *                             not in the columnValueComparers {@link Map}. Can
     *                             be <code>null</code> and will default.
     * @param columnValueComparers {@link Map} of {@link ValueComparer}s to use for
     *                             specific columns. Key is column name, value is
     *                             {@link ValueComparer} to use for comparison of
     *                             that column. Can be <code>null</code> and will
     *                             default to defaultValueComparer for all columns
     *                             in all tables.
     * @since 2.6.0
     */
    public VerifyTableDefinition(final String table, final ColumnValueComparerSource columnValueComparerSource) {
        this(table, null, null, columnValueComparerSource);
    }

    /**
     * Create a valid instance with all columns compared and exclude the specified
     * columns, and use the specified defaultValueComparer for all column
     * comparisons not in the columnValueComparers {@link Map}.
     *
     * @param table                The name of the table - required.
     * @param excludeColumns       The columns in the table to ignore (filter out)
     *                             in expected vs actual comparisons; null or empty
     *                             array to exclude no columns.
     * @param defaultValueComparer {@link ValueComparer} to use with column value
     *                             comparisons when the column name for the table is
     *                             not in the columnValueComparers {@link Map}. Can
     *                             be <code>null</code> and will default.
     * @param columnValueComparers {@link Map} of {@link ValueComparer}s to use for
     *                             specific columns. Key is column name, value is
     *                             {@link ValueComparer} to use for comparison of
     *                             that column. Can be <code>null</code> and will
     *                             default to defaultValueComparer for all columns
     *                             in all tables.
     * @since 2.6.0
     */
    public VerifyTableDefinition(final String table, final String[] excludeColumns,
            final ColumnValueComparerSource columnValueComparerSource) {
        this(table, excludeColumns, null, columnValueComparerSource);
    }

    /**
     * Create a valid instance specifying exclude and include columns.
     *
     * @param table          The name of the table.
     * @param excludeColumns The columns in the table to ignore (filter out) in
     *                       expected vs actual comparisons; null or empty array to
     *                       exclude no columns.
     * @param includeColumns The columns in the table to include in expected vs
     *                       actual comparisons; null to include all columns, empty
     *                       array to include no columns.
     */
    public VerifyTableDefinition(final String table, final String[] excludeColumns, final String[] includeColumns) {
        this(table, excludeColumns, includeColumns, null);
    }

    /**
     * Create a valid instance specifying exclude and include columns and use the
     * specified defaultValueComparer for all column comparisons not in the
     * columnValueComparers {@link Map}.
     *
     * @param table                The name of the table.
     * @param excludeColumns       The columns in the table to ignore (filter out)
     *                             in expected vs actual comparisons; null or empty
     *                             array to exclude no columns.
     * @param includeColumns       The columns in the table to include in expected
     *                             vs actual comparisons; null to include all
     *                             columns, empty array to include no columns.
     * @param defaultValueComparer {@link ValueComparer} to use with column value
     *                             comparisons when the column name for the table is
     *                             not in the columnValueComparers {@link Map}. Can
     *                             be <code>null</code> and will default.
     * @param columnValueComparers {@link Map} of {@link ValueComparer}s to use for
     *                             specific columns. Key is column name, value is
     *                             {@link ValueComparer} to use for comparison of
     *                             that column. Can be <code>null</code> and will
     *                             default to defaultValueComparer for all columns
     *                             in all tables.
     * @since 2.6.0
     */
    public VerifyTableDefinition(final String table, final String[] excludeColumns, final String[] includeColumns,
            final ColumnValueComparerSource columnValueComparerSource) {
        this.columnValueComparerSource = columnValueComparerSource;
        if (table == null) {
            throw new IllegalArgumentException("table is null.");
        }

        tableName = table;
        columnExclusionFilters = excludeColumns;
        columnInclusionFilters = includeColumns;
    }

    public String getTableName() {
        return tableName;
    }

    public String[] getColumnExclusionFilters() {
        return columnExclusionFilters;
    }

    public String[] getColumnInclusionFilters() {
        return columnInclusionFilters;
    }

    public ColumnValueComparerSource getColumnValueComparerSource() {
        return columnValueComparerSource;
    }

    protected String arrayToString(final String[] array) {
        return array == null ? "" : Arrays.toString(array);
    }
}
