/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.database.metadata;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.NoSuchTableException;

/**
 *
 */
public interface TableFinder {

    TableMetadata nameToTable(String freeHandTableName) throws NoSuchTableException, DataSetException;
}
