package org.dbunit.junit4;

import static org.junit.Assert.fail;

import org.dbunit.VerifyTableDefinition;
import org.dbunit.assertion.ComparisonFailure;
import org.dbunit.util.fileloader.DataFileLoader;
import org.dbunit.util.fileloader.FlatXmlDataFileLoader;
import org.junit.Before;

/**
 * Integration test of extends of the PrepAndExpected.
 *
 * @author Jeff Jensen jeffjensen AT users.sourceforge.net
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since 2.4.8
 */
public class DefaultPrepAndExpectedTestCaseExtIT extends DefaultPrepAndExpectedTestCase {

    private static final String PREP_DATA_FILE_NAME = "/xml/flatXmlDataSetTest.xml";
    private static final String EXP_DATA_FILE_NAME = "/xml/flatXmlDataSetTestChanged.xml";

    private static final VerifyTableDefinition TEST_TABLE = makeVerifyTableDefinition("TEST_TABLE");
    private static final VerifyTableDefinition SECOND_TABLE = makeVerifyTableDefinition("SECOND_TABLE");
    private static final VerifyTableDefinition EMPTY_TABLE = makeVerifyTableDefinition("EMPTY_TABLE");
    private static final VerifyTableDefinition PK_TABLE = makeVerifyTableDefinition("PK_TABLE");
    private static final VerifyTableDefinition ONLY_PK_TABLE = makeVerifyTableDefinition("ONLY_PK_TABLE");
    private static final VerifyTableDefinition EMPTY_MULTITYPE_TABLE = makeVerifyTableDefinition(
            "EMPTY_MULTITYPE_TABLE");

    private final DataFileLoader dataFileLoader = new FlatXmlDataFileLoader();

    private static VerifyTableDefinition makeVerifyTableDefinition(final String tableName) {
        return new VerifyTableDefinition(tableName, new String[] {});
    }

    @Before
    public void setUp() throws Exception {
        setDataFileLoader(dataFileLoader);
    }

    public void testSuccessRun() throws Exception {
        final String[] prepDataFiles = { PREP_DATA_FILE_NAME };
        final String[] expectedDataFiles = { PREP_DATA_FILE_NAME };
        final VerifyTableDefinition[] tables = { TEST_TABLE, SECOND_TABLE, EMPTY_TABLE, PK_TABLE, ONLY_PK_TABLE,
                EMPTY_MULTITYPE_TABLE };

        configureTest(prepDataFiles, expectedDataFiles);

        configureVerify(tables);
        verifyData();
    }

    public void testFailRun() throws Exception {
        final String[] prepDataFiles = { PREP_DATA_FILE_NAME };
        final String[] expectedDataFiles = { EXP_DATA_FILE_NAME };
        final VerifyTableDefinition[] tables = { TEST_TABLE, SECOND_TABLE, EMPTY_TABLE, PK_TABLE, ONLY_PK_TABLE,
                EMPTY_MULTITYPE_TABLE };

        configureTest(prepDataFiles, expectedDataFiles);

        try {
            configureVerify(tables);
            verifyData();
            fail("Did not catch expected exception:" + " junit.framework.ComparisonFailure");
        } catch (final ComparisonFailure e) {
            // test passes
        }
    }
}
