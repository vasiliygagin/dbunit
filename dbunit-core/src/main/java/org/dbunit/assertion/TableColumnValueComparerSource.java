/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.assertion;

import static java.util.Collections.emptyMap;

import java.util.HashMap;
import java.util.Map;

import org.dbunit.assertion.comparer.value.DefaultValueComparerDefaults;
import org.dbunit.assertion.comparer.value.ValueComparer;
import org.dbunit.assertion.comparer.value.ValueComparerDefaults;
import org.dbunit.assertion.comparer.value.ValueComparers;

/**
 *
 */
public class TableColumnValueComparerSource {

    private final ValueComparerDefaults valueComparerDefaults;
    private final ValueComparer defaultValueComparer;
    private final Map<String, ColumnValueComparerSource> tableColumnValueComparerSources = new HashMap<>();

    public TableColumnValueComparerSource(ValueComparer defaultValueComparer) {
        if (defaultValueComparer == null) {
            defaultValueComparer = ValueComparers.isActualEqualToExpected;
        }
        this.defaultValueComparer = defaultValueComparer;
        this.valueComparerDefaults = null;
    }

    public TableColumnValueComparerSource() {
        this(ValueComparers.isActualEqualToExpected, emptyMap(), new DefaultValueComparerDefaults());
    }

    public static Map<String, ColumnValueComparerSource> toTableMap() {
        return new HashMap<>();
    }

    public TableColumnValueComparerSource(ValueComparer defaultValueComparer,
            Map<String, ColumnValueComparerSource> tableMap, ValueComparerDefaults valueComparerDefaults) {

        this.valueComparerDefaults = valueComparerDefaults;
        if (defaultValueComparer == null) {
            defaultValueComparer = ValueComparers.isActualEqualToExpected;
        }
        this.defaultValueComparer = defaultValueComparer;

        tableColumnValueComparerSources.putAll(tableMap);
    }

    public ColumnValueComparerSource getColumnValueComparerSource(final String tableName) {
        ColumnValueComparerSource columnValueComparerSource = tableColumnValueComparerSources.get(tableName);
        if (columnValueComparerSource == null) {
            Map<String, ValueComparer> columnValueComparerMapForTable = null;
            if (valueComparerDefaults != null) {
                columnValueComparerMapForTable = valueComparerDefaults
                        .getDefaultColumnValueComparerMapForTable(tableName);
            }
            if (columnValueComparerMapForTable == null) {
                columnValueComparerMapForTable = emptyMap();
            }
            columnValueComparerSource = new ColumnValueComparerSource(defaultValueComparer,
                    columnValueComparerMapForTable);
        }
        return columnValueComparerSource;
    }
}
