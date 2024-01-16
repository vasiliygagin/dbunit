package org.dbunit.junit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.dbunit.Assertion;
import org.dbunit.PrepAndExpectedTestCaseSteps;
import org.dbunit.VerifyTableDefinition;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.statement.IBatchStatement;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.junit4.database.MockDatabaseConnection;
import org.dbunit.junit4.database.statement.MockBatchStatement;
import org.dbunit.junit4.database.statement.MockStatementFactory;
import org.dbunit.util.fileloader.DataFileLoader;
import org.dbunit.util.fileloader.FlatXmlDataFileLoader;

import com.mockobjects.sql.MockConnection;

public class DefaultPrepAndExpectedTestCaseTest {

    private static final String PREP_DATA_FILE_NAME = "/xml/flatXmlDataSetTest.xml";
    private static final String EXP_DATA_FILE_NAME = "/xml/flatXmlDataSetTest.xml";

    private final DataFileLoader dataFileLoader = new FlatXmlDataFileLoader();
    private final DefaultPrepAndExpectedTestCase tc;

    public DefaultPrepAndExpectedTestCaseTest() {
        final IDatabaseConnection onnection = makeDatabaseConnection();
        tc = new DefaultPrepAndExpectedTestCase(dataFileLoader, onnection);
    }

    public void testConfigureTest() throws Exception {

        final String[] prepDataFiles = { PREP_DATA_FILE_NAME };
        final String[] expectedDataFiles = { EXP_DATA_FILE_NAME };
        final VerifyTableDefinition[] tables = {};

        tc.configureTest(tables, prepDataFiles, expectedDataFiles);

        assertEquals("Configured tables do not match expected.", tables, tc.getVerifyTableDefs());

        final IDataSet expPrepDs = dataFileLoader.load(PREP_DATA_FILE_NAME);
        Assertion.assertEquals(expPrepDs, tc.getPrepDataset());

        final IDataSet expExpDs = dataFileLoader.load(EXP_DATA_FILE_NAME);
        Assertion.assertEquals(expExpDs, tc.getExpectedDataset());
    }

    public void testPreTest() throws Exception {
        // TODO implement test
    }

    public void testRunTest() throws Exception {
        final VerifyTableDefinition[] tables = {};
        final String[] prepDataFiles = {};
        final String[] expectedDataFiles = {};
        final PrepAndExpectedTestCaseSteps testSteps = () -> {
            System.out.println("This message represents the test steps.");
            return Boolean.TRUE;
        };

        final Boolean actual = (Boolean) tc.runTest(tables, prepDataFiles, expectedDataFiles, testSteps);

        assertTrue("Did not receive expected value from runTest().", actual);
    }

    public void testPostTest() {
        // TODO implement test
    }

    public void testPostTest_false() {
        // TODO implement test
    }

    public void testSetupData() {
        // TODO implement test
    }

    public void testVerifyData() {
        // TODO implement test
    }

    public void testVerifyDataITableITableStringArrayStringArray() {
        // TODO implement test
    }

    public void testCleanupData() {
        // TODO implement test
    }

    public void testMakeCompositeDataSet() {
        // TODO implement test
    }

    // TODO implement test - doesn't test anything yet
    public void testApplyColumnFiltersBothNull() throws DataSetException {
        final ITable table = new DefaultTable("test_table");
        final String[] excludeColumns = null;
        final String[] includeColumns = null;
        tc.applyColumnFilters(table, excludeColumns, includeColumns);
    }

    // TODO implement test - doesn't test anything yet
    public void testApplyColumnFiltersBothNotNull() throws DataSetException {
        final ITable table = new DefaultTable("test_table");
        final String[] excludeColumns = { "COL1" };
        final String[] includeColumns = { "COL2" };
        tc.applyColumnFilters(table, excludeColumns, includeColumns);
    }

    protected IDatabaseConnection makeDatabaseConnection() {
        final MockConnection mockConnection = new MockConnection();

        final MockStatementFactory mockStatementFactory = new MockStatementFactory();
        final IBatchStatement mockBatchStatement = new MockBatchStatement();
        mockStatementFactory.setupStatement(mockBatchStatement);

        final MockDatabaseConnection mockDbConnection = new MockDatabaseConnection();
        mockDbConnection.setupConnection(mockConnection);
        mockDbConnection.setupStatementFactory(mockStatementFactory);

        mockDbConnection.getDatabaseConfig().setCaseSensitiveTableNames(true);

        return mockDbConnection;
    }
}
