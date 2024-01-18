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

import java.sql.Connection;
import java.sql.SQLException;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.AbstractDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decorates an operation and executes within the context of a transaction.
 *
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Feb 21, 2002
 */
public class TransactionOperation extends DatabaseOperation {

    /**
     * Logger for this class
     */
    private static final Logger logger = LoggerFactory.getLogger(TransactionOperation.class);

    private final DatabaseOperation _operation;

    /**
     * Creates a TransactionOperation that decorates the specified operation.
     */
    public TransactionOperation(DatabaseOperation operation) {
        _operation = operation;
    }

    ////////////////////////////////////////////////////////////////////////////
    // DatabaseOperation class

    @Override
    public void execute(AbstractDatabaseConnection connection, IDataSet dataSet)
            throws DatabaseUnitException, SQLException {
        logger.debug("execute(connection={}, dataSet={}) - start", connection, dataSet);

        Connection jdbcConnection = connection.getConnection();

        if (jdbcConnection.getAutoCommit() == false) {
            throw new ExclusiveTransactionException();
        }

        jdbcConnection.setAutoCommit(false);
        try {
            _operation.execute(connection, dataSet);
            jdbcConnection.commit();
        } catch (DatabaseUnitException | SQLException | RuntimeException e) {
            jdbcConnection.rollback();
            throw e;
        } finally {
            jdbcConnection.setAutoCommit(true);
        }
    }
}
