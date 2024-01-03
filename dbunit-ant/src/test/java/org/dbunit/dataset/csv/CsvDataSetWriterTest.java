package org.dbunit.dataset.csv;

import java.io.File;
import java.io.IOException;

import org.dbunit.Assertion;
import org.dbunit.dataset.CachedDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DataSetUtils;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.testutil.TestUtils;
import org.dbunit.util.FileHelper;

import junit.framework.TestCase;

/**
 * Created By: fede Date: 10-mar-2004 Time: 17.21.34
 *
 * Last Checkin: $Author$ Date: $Date$ Revision: $Revision$
 */
public class CsvDataSetWriterTest extends TestCase {
    private static final String DEST = "target/csv/orders-out";
    private static final String SOURCE = TestUtils.getFileName("csv/orders");
    protected static final File DATASET_DIR = TestUtils.getFile("csv/orders");

    public void testProduceAndWriteBackToDisk() throws Exception {
        produceToFolder(SOURCE, DEST);
        IDataSet expected = produceToMemory(SOURCE);
        IDataSet actual = produceToMemory(DEST);
        Assertion.assertEquals(expected, actual);
    }

    private IDataSet produceToMemory(String source) throws DataSetException {
        CsvProducer producer = new CsvProducer(source);
        CachedDataSet cached = new CachedDataSet();
        producer.produce(cached);
        return cached;
    }

    private void produceToFolder(String source, String dest) throws DataSetException {
        CsvProducer producer = new CsvProducer(source);
        new File(dest).delete();
        CsvDataSetWriter writer = new CsvDataSetWriter(dest);
        producer.produce(writer);
    }

    public void testEscapeQuote() {
        assertEquals("\\\"foo\\\"", CsvDataSetWriter.escape("\"foo\""));
    }

    public void testEscapeEscape() {
        assertEquals("\\\\foo\\\\", CsvDataSetWriter.escape("\\foo\\"));
    }

    public void testWrite() throws Exception {

        IDataSet expectedDataSet = new CsvDataSet(DATASET_DIR);

        File tempDir = createTmpDir();
        try {
            CsvDataSetWriter writer = new CsvDataSetWriter(tempDir);
            writer.write(expectedDataSet);

            File tableOrderingFile = new File(tempDir, CsvDataSet.TABLE_ORDERING_FILE);
            assertTrue(tableOrderingFile.exists());

            IDataSet actualDataSet = new CsvDataSet(tempDir);

            // verify table count
            assertEquals("table count", expectedDataSet.getTableNames().length, actualDataSet.getTableNames().length);

            // verify each table
            ITable[] expected = DataSetUtils.getTables(expectedDataSet);
            ITable[] actual = DataSetUtils.getTables(actualDataSet);
            assertEquals("table count", expected.length, actual.length);
            for (int i = 0; i < expected.length; i++) {
                String expectedName = expected[i].getTableMetaData().getTableName();
                String actualName = actual[i].getTableMetaData().getTableName();
                assertEquals("table name", expectedName, actualName);

                assertTrue("not same instance", expected[i] != actual[i]);
                Assertion.assertEquals(expected[i], actual[i]);
            }

        } finally {
            FileHelper.deleteDirectory(tempDir, true);

        }

        // assertFalse("temporary directory was not deleted", tempDir.exists());
    }

    private File createTmpDir() throws IOException {
        File tmpFile = File.createTempFile("CsvDataSetTest", "-csv");
        String fullPath = tmpFile.getAbsolutePath();
        tmpFile.delete();

        File tmpDir = new File(fullPath);
        if (!tmpDir.mkdir()) {
            throw new IOException("Failed to create tmpDir: " + fullPath);
        }

        return tmpDir;
    }
}
