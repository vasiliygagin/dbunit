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
package org.dbunit.operation;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.AbstractDatabaseConnection;
import org.dbunit.database.statement.IBatchStatement;
import org.dbunit.database.statement.IStatementFactory;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITableIterator;

/**
 * Deletes all rows of tables present in the specified dataset. If the dataset
 * does not contains a particular table, but that table exists in the database,
 * the database table is not affected. Table are truncated in reverse sequence.
 * <p/>
 * This operation has the same effect of as {@link TruncateTableOperation}.
 * TruncateTableOperation is faster, and it is non-logged, meaning it cannot be
 * rollback. DeleteAllOperation is more portable because not all database vendor
 * support TRUNCATE_TABLE TABLE statement.
 *
 * @author Manuel Laflamme
 * @version $Revision$
 * @see TruncateTableOperation
 * @since Feb 18, 2002
 */
public class DeleteAllOperation extends AbstractOperation {

    ////////////////////////////////////////////////////////////////////////////
    // DatabaseOperation class

    @Override
    public void execute(AbstractDatabaseConnection connection, IDataSet dataSet)
            throws DatabaseUnitException, SQLException {
        IStatementFactory statementFactory = connection.getDatabaseConfig().getStatementFactory();
        IBatchStatement statement = statementFactory.createBatchStatement(connection);
        try {
            int count = 0;

            Set<String> allTableNames = getAllTableNames(dataSet);
            for (String tableName : allTableNames) {

                // Use database table name. Required to support case sensitive database.
                String databaseTableName = connection.correctTableName(tableName);

                String sql = buildDeleteSql(databaseTableName);
                statement.addBatch(sql);

                count++;
            }

            if (count > 0) {
                statement.executeBatch();
                statement.clearBatch();
            }
        } finally {
            statement.close();
        }
    }

    protected String buildDeleteSql(String tableName) {
        return "delete from " + tableName;
    }

    Set<String> getAllTableNames(IDataSet dataSet) throws DataSetException {
        Set<String> tableNames = new HashSet<>();
        ITableIterator iterator = dataSet.iterator();
        while (iterator.next()) {
            tableNames.add(iterator.getTableMetaData().getTableName());
        }

        return tableNames;
    }
}
