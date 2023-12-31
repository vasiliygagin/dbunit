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

package org.dbunit.dataset.xml;

import java.io.InputStream;
import java.io.Reader;

import org.dbunit.dataset.AbstractDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.DefaultTableIterator;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.NoSuchTableException;
import org.dbunit.dataset.OrderedTableNameMap;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

/**
 * @author Manuel Laflamme
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since 1.0 (Apr 4, 2002)
 */
public class FlatDtdDataSet extends AbstractDataSet implements IDataSetConsumer {
    private static final Logger logger = LoggerFactory.getLogger(FlatDtdDataSet.class);

    private boolean _ready = false;

    public FlatDtdDataSet() {
        initialize();
    }

    public FlatDtdDataSet(InputStream in) throws DataSetException {
        this(new FlatDtdProducer(new InputSource(in)));
    }

    public FlatDtdDataSet(Reader reader) throws DataSetException {
        this(new FlatDtdProducer(new InputSource(reader)));
    }

    public FlatDtdDataSet(IDataSetProducer producer) throws DataSetException {
        initialize();
        producer.produce(this);
    }

    @Override
    protected void initialize() {
        if (_orderedTableNameMap == null) {
            _orderedTableNameMap = new OrderedTableNameMap<>(isCaseSensitiveTableNames());
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // AbstractDataSet class

    @Override
    protected ITableIterator createIterator(boolean reversed) throws DataSetException {
        if (logger.isDebugEnabled())
            logger.debug("createIterator(reversed={}) - start", String.valueOf(reversed));

        // Verify producer notifications completed
        if (!_ready) {
            throw new IllegalStateException("Not ready!");
        }

        String[] names = _orderedTableNameMap.getTableNames();
        ITable[] tables = new ITable[names.length];
        for (int i = 0; i < names.length; i++) {
            String tableName = names[i];
            ITable table = _orderedTableNameMap.get(tableName);
            if (table == null) {
                throw new NoSuchTableException(tableName);
            }

            tables[i] = table;
        }

        return new DefaultTableIterator(tables, reversed);
    }

    ////////////////////////////////////////////////////////////////////////////
    // IDataSet interface

    @Override
    public String[] getTableNames() throws DataSetException {
        logger.debug("getTableNames() - start");

        // Verify producer notifications completed
        if (!_ready) {
            throw new IllegalStateException("Not ready!");
        }

        return _orderedTableNameMap.getTableNames();
    }

    @Override
    public ITableMetaData getTableMetaData(String tableName) throws DataSetException {
        logger.debug("getTableMetaData(tableName={}) - start", tableName);

        // Verify producer notifications completed
        if (!_ready) {
            throw new IllegalStateException("Not ready!");
        }

        return super.getTableMetaData(tableName);
    }

    @Override
    public ITable getTable(String tableName) throws DataSetException {
        logger.debug("getTable(tableName={}) - start", tableName);

        // Verify producer notifications completed
        if (!_ready) {
            throw new IllegalStateException("Not ready!");
        }

        return super.getTable(tableName);
    }

    ////////////////////////////////////////////////////////////////////////
    // IDataSetConsumer interface

    @Override
    public void startDataSet() throws DataSetException {
        logger.debug("startDataSet() - start");

        _ready = false;
    }

    @Override
    public void endDataSet() throws DataSetException {
        logger.debug("endDataSet() - start");

        _ready = true;
    }

    @Override
    public void startTable(ITableMetaData metaData) throws DataSetException {
        logger.debug("startTable(metaData={}) - start", metaData);

        String tableName = metaData.getTableName();
        _orderedTableNameMap.add(tableName, new DefaultTable(metaData));
    }

    @Override
    public void endTable() throws DataSetException {
        // no op
    }

    @Override
    public void row(Object[] values) throws DataSetException {
        // no op
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(getClass().getName()).append("[");
        sb.append("_ready=").append(this._ready);
        sb.append(", _orderedTableNameMap=").append(this._orderedTableNameMap);
        sb.append("]");
        return sb.toString();
    }
}