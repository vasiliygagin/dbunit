package org.dbunit.database.search;

import java.sql.SQLException;

import org.dbunit.database.AbstractImportedKeysFilteredByPKsTestCase;
import org.dbunit.dataset.DataSetException;
import org.dbunit.util.search.SearchException;
import org.junit.Test;

public class ImportedKeysFilteredByPKsSingleTest extends AbstractImportedKeysFilteredByPKsTestCase {

    public ImportedKeysFilteredByPKsSingleTest() {
        super("hypersonic_simple_dataset.sql");
    }

    @Override
    protected int[] setupTablesSizeFixture() {
        int[] sizes = { 1, 1, 2 };
        return sizes;
    }

    @Test
    public void testCWithOne() throws DataSetException, SQLException, SearchException {
        addInput(C, new String[] { C1 });
        addOutput(C, new String[] { C1 });
        doIt();
    }

    @Test
    public void testCWithTwo() throws DataSetException, SQLException, SearchException {
        addInput(C, new String[] { C1, C2 });
        addOutput(C, new String[] { C1, C2 });
        doIt();
    }

    @Test
    public void testCWithTwoInvertedInput() throws DataSetException, SQLException, SearchException {
        addInput(C, new String[] { C2, C1 });
        addOutput(C, new String[] { C1, C2 });
        doIt();
    }

    @Test
    public void testCWithTwoInvertedOutput() throws DataSetException, SQLException, SearchException {
        addInput(C, new String[] { C1, C2 });
        addOutput(C, new String[] { C2, C1 });
        doIt();
    }

    @Test
    public void testCWithRepeated() throws DataSetException, SQLException, SearchException {
        addInput(C, new String[] { C1, C2, C1, C1, C2, C2, C2, C1, C1, C2 });
        addOutput(C, new String[] { C2, C1 });
        doIt();
    }

    @Test
    public void testB() throws DataSetException, SQLException, SearchException {
        addInput(B, new String[] { B1 });
        addOutput(C, new String[] { C2 });
        addOutput(B, new String[] { B1 });
        doIt();
    }

    @Test
    public void testBWithRepeated() throws DataSetException, SQLException, SearchException {
        addInput(B, new String[] { B1, B1, B1, B1 });
        addOutput(C, new String[] { C2 });
        addOutput(B, new String[] { B1 });
        doIt();
    }

    @Test
    public void testA() throws DataSetException, SQLException, SearchException {
        addInput(A, new String[] { A1 });
        addOutput(C, new String[] { C1, C2 });
        addOutput(B, new String[] { B1 });
        addOutput(A, new String[] { A1 });
        doIt();
    }

    @Test
    public void testAWithRepeated() throws DataSetException, SQLException, SearchException {
        addInput(A, new String[] { A1, A1, A1 });
        addOutput(C, new String[] { C1, C2 });
        addOutput(B, new String[] { B1 });
        addOutput(A, new String[] { A1 });
        doIt();
    }

}
