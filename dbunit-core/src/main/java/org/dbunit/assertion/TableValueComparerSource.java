/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.assertion;

import java.util.Map;

import org.dbunit.assertion.comparer.value.ValueComparer;
import org.dbunit.assertion.comparer.value.ValueComparerDefaults;

/**
 *
 */
public class TableValueComparerSource {

    private final ValueComparerDefaults valueComparerDefaults;
    private final ValueComparer defaultValueComparer;
    private final Map<String, ValueComparer> columnValueComparers;

    public TableValueComparerSource(ValueComparerDefaults valueComparerDefaults, ValueComparer defaultValueComparer,
            Map<String, ValueComparer> columnValueComparers) {
        this.valueComparerDefaults = valueComparerDefaults;
        this.defaultValueComparer = defaultValueComparer;
        this.columnValueComparers = columnValueComparers;
    }

    public ColumnValueComparerSource selectColumnValueComparer(final String expectedTableName) {

        final ValueComparer validDefaultValueComparer;
        if (defaultValueComparer == null) {
            validDefaultValueComparer = valueComparerDefaults.getDefaultValueComparer();
        } else {
            validDefaultValueComparer = defaultValueComparer;
        }

        final Map<String, ValueComparer> validColumnValueComparers;
        if (columnValueComparers == null) {
            validColumnValueComparers = valueComparerDefaults
                    .getDefaultColumnValueComparerMapForTable(expectedTableName);
        } else {
            validColumnValueComparers = columnValueComparers;
        }

        return new ColumnValueComparerSource(validDefaultValueComparer, validColumnValueComparers);
    }

}
