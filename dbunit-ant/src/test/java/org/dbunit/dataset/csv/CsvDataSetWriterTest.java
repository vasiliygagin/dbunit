package org.dbunit.dataset.csv;

import java.io.File;
import java.io.IOException;

import org.dbunit.Assertion;
import org.dbunit.dataset.CachedDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;

import junit.framework.TestCase;

/**
 * Created By: fede Date: 10-mar-2004 Time: 17.21.34
 *
 * Last Checkin: $Author$ Date: $Date$ Revision: $Revision$
 */
public class CsvDataSetWriterTest extends TestCase {

    public void testProduceAndWriteBackToDisk() throws Exception {
        produceToFolder("src/test/resources/csv/orders", "target/csv/orders-out");
        IDataSet expected = produceToMemory("src/test/resources/csv/orders");
        IDataSet actual = produceToMemory("target/csv/orders-out");
        Assertion.assertEquals(expected, actual);
    }

    private IDataSet produceToMemory(String source) throws DataSetException {
        CsvProducer producer = new CsvProducer(new File(source));
        CachedDataSet cached = new CachedDataSet();
        producer.produce(cached);
        return cached;
    }

    private void produceToFolder(String source, String dest) throws DataSetException {
        CsvProducer producer = new CsvProducer(new File(source).getAbsoluteFile());
        File destFile = new File(dest).getAbsoluteFile();
        destFile.delete();
        CsvDataSetWriter writer = new CsvDataSetWriter(destFile);
        producer.produce(writer);
    }

    public void testEscapeQuote() {
        assertEquals("\\\"foo\\\"", CsvDataSetWriter.escape("\"foo\""));
    }

    public void testEscapeEscape() {
        assertEquals("\\\\foo\\\\", CsvDataSetWriter.escape("\\foo\\"));
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
