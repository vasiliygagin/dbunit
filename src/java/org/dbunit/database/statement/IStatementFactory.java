/*
 * IStatementFactory.java   Mar 16, 2002
 *
 * The dbUnit database testing framework.
 * Copyright (C) 2002   Manuel Laflamme
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

package org.dbunit.database.statement;

import java.sql.SQLException;

import org.dbunit.database.IDatabaseConnection;

/**
 * @author Manuel Laflamme
 * @version 1.0
 */
public interface IStatementFactory
{
    IBatchStatement createBatchStatement(IDatabaseConnection connection)
            throws SQLException;

    IPreparedBatchStatement createPreparedStatement(String sql,
            IDatabaseConnection connection) throws SQLException;
}
