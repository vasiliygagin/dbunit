/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.database.metadata;

/**
 * Predicate used by {@link MetadataManager} to see if table needs to be ignored
 */
@FunctionalInterface
public interface IgnoredTablePredicate {

    public static final IgnoredTablePredicate ALLOW_ALL = T -> false;

    boolean shouldIgnore(TableMetadata tableMetadata);
}
