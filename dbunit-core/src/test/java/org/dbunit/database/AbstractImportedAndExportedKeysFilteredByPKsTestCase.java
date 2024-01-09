package org.dbunit.database;

import java.sql.SQLException;

import org.dbunit.database.search.TablesDependencyHelper;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.util.search.SearchException;

public abstract class AbstractImportedAndExportedKeysFilteredByPKsTestCase
        extends AbstractSearchCallbackFilteredByPKsTestCase {

    public AbstractImportedAndExportedKeysFilteredByPKsTestCase(String sqlFile) throws Exception {
        super(sqlFile);
    }

    @Override
    protected IDataSet getDataset() throws SQLException, SearchException, DataSetException {
        return TablesDependencyHelper.getAllDataset(database.getConnection(), getInput());
    }

}
