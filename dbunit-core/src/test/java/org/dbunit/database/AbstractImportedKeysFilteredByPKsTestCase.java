package org.dbunit.database;

import java.sql.SQLException;

import org.dbunit.database.search.TablesDependencyHelper;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.util.search.SearchException;

public abstract class AbstractImportedKeysFilteredByPKsTestCase extends AbstractSearchCallbackFilteredByPKsTestCase {

    public AbstractImportedKeysFilteredByPKsTestCase(String sqlFile) throws Exception {
        super(sqlFile);
    }

    @Override
    protected IDataSet getDataset() throws SQLException, SearchException, DataSetException {
        return TablesDependencyHelper.getDataset(getConnection(), getInput());
    }

}
