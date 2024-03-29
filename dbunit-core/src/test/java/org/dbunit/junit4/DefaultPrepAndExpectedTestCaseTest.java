package org.dbunit.junit4;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;

import org.dbunit.Assertion;
import org.dbunit.PrepAndExpectedTestCaseSteps;
import org.dbunit.VerifyTableDefinition;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.junit.DbUnitTestFacade;
import org.dbunit.util.fileloader.DataFileLoader;
import org.dbunit.util.fileloader.FlatXmlDataFileLoader;
import org.junit.Test;

public class DefaultPrepAndExpectedTestCaseTest {

    private static final String PREP_DATA_FILE_NAME = "/xml/flatXmlDataSetTest.xml";
    private static final String EXP_DATA_FILE_NAME = "/xml/flatXmlDataSetTest.xml";

    private final DataFileLoader dataFileLoader = new FlatXmlDataFileLoader();
    private final DefaultPrepAndExpectedTestCase tc;
    private final DbUnitTestFacade dbUnitTestFacade = mock(DbUnitTestFacade.class);

    public DefaultPrepAndExpectedTestCaseTest() throws Exception {
        tc = new DefaultPrepAndExpectedTestCase();
        tc.setDataFileLoader(dataFileLoader);

        Field dbUnitField = DefaultPrepAndExpectedTestCase.class.getDeclaredField("dbUnit");
        dbUnitField.setAccessible(true);
        dbUnitField.set(tc, dbUnitTestFacade);
    }

    @Test
    public void testConfigureTest() throws Exception {

        final String[] prepDataFiles = { PREP_DATA_FILE_NAME };
        final String[] expectedDataFiles = { EXP_DATA_FILE_NAME };
        final VerifyTableDefinition[] tables = {};

        tc.configureTest(prepDataFiles, expectedDataFiles);

        final IDataSet expPrepDs = dataFileLoader.load(PREP_DATA_FILE_NAME);
        Assertion.assertEquals(expPrepDs, tc.getPrepDataset());

        final IDataSet expExpDs = dataFileLoader.load(EXP_DATA_FILE_NAME);
        Assertion.assertEquals(expExpDs, tc.getExpectedDataset());
    }

    @Test
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

    // TODO implement test - doesn't test anything yet
    @Test
    public void testApplyColumnFiltersBothNull() throws DataSetException {
        final ITable table = new DefaultTable("test_table");
        final String[] excludeColumns = null;
        final String[] includeColumns = null;
        tc.applyColumnFilters(table, excludeColumns, includeColumns);
    }

    // TODO implement test - doesn't test anything yet
    @Test
    public void testApplyColumnFiltersBothNotNull() throws DataSetException {
        final ITable table = new DefaultTable("test_table");
        final String[] excludeColumns = { "COL1" };
        final String[] includeColumns = { "COL2" };
        tc.applyColumnFilters(table, excludeColumns, includeColumns);
    }
}
