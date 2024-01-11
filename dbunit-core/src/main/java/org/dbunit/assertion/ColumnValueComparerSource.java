/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.assertion;

import java.util.Map;

import org.dbunit.assertion.comparer.value.ValueComparer;

/**
 *
 */
public class ColumnValueComparerSource {

    private final ValueComparer defaultValueComparer;
    private final Map<String, ValueComparer> columnValueComparers;

    /**
     * @param defaultValueComparer
     * @param columnValueComparers
     */
    public ColumnValueComparerSource(ValueComparer defaultValueComparer,
            Map<String, ValueComparer> columnValueComparers) {
        this.defaultValueComparer = defaultValueComparer;
        this.columnValueComparers = columnValueComparers;
    }

    public ValueComparer selectValueComparer(final String columnName) {
        ValueComparer valueComparer = columnValueComparers.get(columnName);
        if (valueComparer == null) {
            valueComparer = defaultValueComparer;
        }

        return valueComparer;
    }
}
