package org.dbunit.assertion.comparer.value.builder;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.dbunit.assertion.ColumnValueComparerSource;
import org.dbunit.assertion.TableColumnValueComparerSource;
import org.dbunit.assertion.comparer.value.ValueComparer;

/**
 * Convenience methods to help build the map of table name -> map of column name
 * -> {@link ValueComparer}.
 *
 * @author Jeff Jensen
 * @since 2.6.0
 */
public class TableColumnValueComparerMapBuilder {

    private ValueComparer defaultValueComparer = null;
    private Map<String, Map<String, ValueComparer>> comparers = new HashMap<>();

    /**
     * Add all mappings from the specified table map to this builder.
     *
     * @return this for fluent syntax.
     */
    public TableColumnValueComparerMapBuilder add(
            final Map<String, Map<String, ValueComparer>> tableColumnValueComparers) {
        comparers.putAll(tableColumnValueComparers);
        return this;
    }

    /**
     * Add all mappings from the specified column map to a column map for the
     * specified table in this builder.
     *
     * @return this for fluent syntax.
     */
    public TableColumnValueComparerMapBuilder add(final String tableName,
            final Map<String, ValueComparer> columnValueComparers) {
        final Map<String, ValueComparer> map = findOrMakeColumnMap(tableName);

        map.putAll(columnValueComparers);

        return this;
    }

    /**
     * Add a table to column to {@link ValueComparer} mapping.
     *
     * @return this for fluent syntax.
     */
    public TableColumnValueComparerMapBuilder add(final String tableName, final String columnName,
            final ValueComparer valueComparer) {
        final Map<String, ValueComparer> map = findOrMakeColumnMap(tableName);
        map.put(columnName, valueComparer);
        return this;
    }

    /** @return The unmodifiable assembled map. */
    public TableColumnValueComparerSource build() {
        Map<String, ColumnValueComparerSource> tableMap = new HashMap<>();
        if (comparers != null) {
            for (Entry<String, Map<String, ValueComparer>> e : comparers.entrySet()) {
                String tableName = e.getKey();
                Map<String, ValueComparer> columnValueCompareres = e.getValue();

                ColumnValueComparerSource columnValueComparerSource = new ColumnValueComparerSource(
                        defaultValueComparer, columnValueCompareres);
                tableMap.put(tableName, columnValueComparerSource);
            }
        }
        return new TableColumnValueComparerSource(defaultValueComparer, tableMap, null);
    }

    Map<String, ValueComparer> findOrMakeColumnMap(final String tableName) {
        Map<String, ValueComparer> map = comparers.get(tableName);
        if (map == null) {
            map = new HashMap<>();
            comparers.put(tableName, map);
        }
        return map;
    }
}
