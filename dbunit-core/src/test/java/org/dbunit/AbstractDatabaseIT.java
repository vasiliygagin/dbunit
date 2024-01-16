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

package org.dbunit;

import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.SortedTable;
import org.dbunit.operation.DatabaseOperation;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Feb 18, 2002
 */
public abstract class AbstractDatabaseIT extends AbstractDatabaseTest {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    public AbstractDatabaseIT() throws Exception {
    }

    @Before
    public final void setUp() throws Exception {
        dbUnit.executeOperation(DatabaseOperation.CLEAN_INSERT, getDataSet());
    }

    protected ITable createOrderedTable(String tableName, String orderByColumn) throws Exception {
        return new SortedTable(database.getConnection().createDataSet().getTable(tableName),
                new String[] { orderByColumn });
//        String sql = "select * from " + tableName + " order by " + orderByColumn;
//        return _connection.createQueryTable(tableName, sql);
    }

    /**
     * Returns the string converted as an identifier according to the metadata rules
     * of the database environment. Most databases convert all metadata identifiers
     * to uppercase. PostgreSQL converts identifiers to lowercase. MySQL preserves
     * case.
     *
     * @param str The identifier.
     * @return The identifier converted according to database rules.
     */
    protected String convertString(String str) throws Exception {
        return environment.convertString(str);
    }

    protected IDataSet getDataSet() throws Exception {
        return environment.getInitDataSet();
    }
}
