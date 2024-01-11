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
package org.dbunit.dataset.filter;

import java.util.ArrayList;
import java.util.List;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITableIterator;

/**
 * This class provides a skeletal implementation of the {@link ITableFilter}
 * interface to minimize the effort required to implement a filter. Subclasses
 * are only required to implement the {@link #isValidName} method.
 *
 * @author Manuel Laflamme
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since 2.2.0
 */
public abstract class AbstractTableFilter implements ITableFilter {

    /**
     * Returns <code>true</code> if specified table is allowed by this filter. This
     * legacy method, now replaced by accept, still exist for compatibily with older
     * environment
     */
    public final boolean isValidName(String tableName) throws DataSetException {
        return accept(tableName);
    }

    @Override
    public String[] getTableNames(IDataSet dataSet) throws DataSetException {
        String[] tableNames = dataSet.getTableNames();
        List<String> nameList = new ArrayList<>();
        for (String tableName : tableNames) {
            if (accept(tableName)) {
                nameList.add(tableName);
            }
        }
        return nameList.toArray(new String[0]);
    }

    @Override
    public ITableIterator iterator(IDataSet dataSet, boolean reversed) throws DataSetException {
        ITableIterator iterator = reversed ? dataSet.reverseIterator() : dataSet.iterator();
        return new FilteredTableIterator(iterator, this);
    }
}
