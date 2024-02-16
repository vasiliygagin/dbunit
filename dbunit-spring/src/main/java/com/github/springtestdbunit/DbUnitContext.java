package com.github.springtestdbunit;

import com.github.springtestdbunit.dataset.DataSetLoader;
import com.github.springtestdbunit.operation.DatabaseOperationLookup;

/**
 * @deprecated Not used anymore will remove ASAP
 */
@Deprecated
public class DbUnitContext {

    private DataSetLoader dataSetLoader;
    private DatabaseOperationLookup databaseOperationLookup;

    public DataSetLoader getDataSetLoader() {
	return dataSetLoader;
    }

    public void setDataSetLoader(DataSetLoader dataSetLoader) {
	this.dataSetLoader = dataSetLoader;
    }

    public DatabaseOperationLookup getDatabaseOperationLookup() {
	return databaseOperationLookup;
    }

    public void setDatabaseOperationLookup(DatabaseOperationLookup databaseOperationLookup) {
	this.databaseOperationLookup = databaseOperationLookup;
    }
}
