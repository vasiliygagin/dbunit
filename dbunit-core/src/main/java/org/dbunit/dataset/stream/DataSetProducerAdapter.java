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
package org.dbunit.dataset.stream;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.RowOutOfBoundsException;

/**
 * Implementation of {@link IDataSetProducer} based on a given {@link IDataSet}
 * or a {@link ITableIterator}.
 * 
 * @author Manuel Laflamme
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since Apr 17, 2003
 */
public class DataSetProducerAdapter implements IDataSetProducer {

    private final ITableIterator _iterator;
    private IDataSetConsumer _consumer = DefaultConsumer.NO_OP_CONSUMER;

    public DataSetProducerAdapter(ITableIterator iterator) {
        _iterator = iterator;
    }

    public DataSetProducerAdapter(IDataSet dataSet) throws DataSetException {
        this(dataSet.iterator());
    }

    @Override
    public void setConsumer(IDataSetConsumer consumer) throws DataSetException {
        _consumer = consumer;
    }

    @Override
    public void produce() throws DataSetException {
        _consumer.startDataSet();
        while (_iterator.next()) {
            produceTable(_iterator.getTable());
        }
        _consumer.endDataSet();
    }

    private void produceTable(ITable table) throws DataSetException {
        ITableMetaData metaData = table.getTableMetaData();

        _consumer.startTable(metaData);
        try {
            Column[] columns = metaData.getColumns();
            if (columns.length == 0) {
                _consumer.endTable();
                return;
            }

            for (int i = 0;; i++) {
                produceRow(table, columns, i);
            }
        } catch (RowOutOfBoundsException e) {
            // This exception occurs when records are exhausted
            // and we reach the end of the table. Ignore this error
            // and close table.

            // end of table
            _consumer.endTable();
        }
    }

    private void produceRow(ITable table, Column[] columns, int i) throws DataSetException {
        Object[] values = new Object[columns.length];
        for (int j = 0; j < columns.length; j++) {
            Column column = columns[j];
            values[j] = table.getValue(i, column.getColumnName());
        }
        _consumer.row(values);
    }
}
