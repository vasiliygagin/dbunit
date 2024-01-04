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
package org.dbunit.dataset.excel;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.dbunit.dataset.AbstractDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTableIterator;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;
import org.dbunit.dataset.OrderedTableNameMap;

/**
 * This dataset implementation can read and write MS Excel documents. Each sheet
 * represents a table. The first row of a sheet defines the columns names and
 * remaining rows contains the data.
 *
 * @author Manuel Laflamme
 * @since Feb 21, 2003
 * @version $Revision$
 * @deprecated Keeping for compatibility
 */
@Deprecated
public class XlsDataSet extends AbstractDataSet {

    private final OrderedTableNameMap<ITable> _tables;

    /**
     * Creates a new XlsDataSet object that loads the specified Excel document.
     */
    public XlsDataSet(File file) throws IOException, DataSetException {
        this(new FileInputStream(file));
    }

    /**
     * Creates a new XlsDataSet object that loads the specified Excel document.
     */
    public XlsDataSet(InputStream in) throws IOException, DataSetException {
        _tables = new OrderedTableNameMap<>(isCaseSensitiveTableNames());

        Workbook workbook;
        try {
            workbook = WorkbookFactory.create(in);
        } catch (EncryptedDocumentException e) {
            throw new IOException(e);
        }

        int sheetCount = workbook.getNumberOfSheets();
        for (int i = 0; i < sheetCount; i++) {
            ITable table = new XlsTable(workbook.getSheetName(i), workbook.getSheetAt(i));
            _tables.add(table.getTableMetaData().getTableName(), table);
        }
    }

    @Override
    protected ITableIterator createIterator(boolean reversed) throws DataSetException {
        ITable[] tables = _tables.orderedValues().toArray(new ITable[0]);
        return new DefaultTableIterator(tables, reversed);
    }
}
