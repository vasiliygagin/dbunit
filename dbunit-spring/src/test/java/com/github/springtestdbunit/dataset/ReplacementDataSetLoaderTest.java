package com.github.springtestdbunit.dataset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.context.TestContext;

import com.github.springtestdbunit.testutils.ExtendedTestContextManager;

/**
 * Tests for {@link ReplacementDataSetLoader}.
 *
 * @author Stijn Van Bael
 */
public class ReplacementDataSetLoaderTest {

    private ReplacementDataSetLoader loader = new ReplacementDataSetLoader();

    private TestContext testContext;

    @Before
    public void setup() throws Exception {
        ExtendedTestContextManager manager = new ExtendedTestContextManager(getClass());
        this.testContext = manager.accessTestContext();
    }

    @Test
    public void shouldReplaceNulls() throws Exception {
        IDataSet dataset = this.loader.loadDataSet(this.testContext.getTestClass(), "test-replacement.xml");
        assertEquals("Sample", dataset.getTableNames()[0]);
        ITable table = dataset.getTable("Sample");
        assertEquals(1, table.getRowCount());
        assertNull(table.getValue(0, "value"));
    }

}
