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

package org.dbunit.database;

import java.sql.Connection;
import java.sql.SQLException;

import org.dbunit.database.statement.IStatementFactory;

/**
 * This interface represents a connection to a specific database.
 *
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Mar 6, 2002
 */
public interface IDatabaseConnection {

    /**
     * Returns a JDBC database connection.
     */
    public Connection getConnection() throws SQLException;

    /**
     * Returns the database schema name.
     */
    public String getSchema();

    /**
     * Close this connection.
     */
    public void close() throws SQLException;

    /**
     * Returns the specified table row count.
     *
     * @param tableName the table name
     * @return the row count
     */
    public int getRowCount(String tableName) throws SQLException;

    /**
     * Returns the specified table row count according specified where clause.
     *
     * @param tableName   the table name
     * @param whereClause the where clause
     * @return the row count
     */
    public int getRowCount(String tableName, String whereClause) throws SQLException;

    io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig getDatabaseConfig();

    /**
     * Returns this connection database configuration
     * @deprecated Use {@link #getDatabaseConfig()}
     */
    @Deprecated
    default DatabaseConfig getConfig() {
        return new DatabaseConfigWrapper(getDatabaseConfig());
    }

    /**
     * @deprecated Use {@link #getDatabaseConfig()}
     */
    @Deprecated
    default IStatementFactory getStatementFactory() {
        return getDatabaseConfig().getStatementFactory();
    }
}
