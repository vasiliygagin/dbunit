/*
 *
 *  The DbUnit Database Testing Framework
 *  Copyright (C)2002-2008, DbUnit.org
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */
package org.dbunit.assertion;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.dataset.xml.XmlUtil;
import org.junit.Test;

/**
 * @author gommma (gommma AT users.sourceforge.net)
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since 2.4.0
 */
public class DiffCollectingFailureHandlerTest {

    private DbUnitAssert assertion = new DbUnitAssert();

    @Test
    public void testAssertTablesWithDifferentValues() throws Exception {
        String fileName = "src/test/resources/xml/assertionTest.xml";
        IDataSet dataSet = new FlatXmlDataSet(XmlUtil.buildInputSourceFromFile(fileName));
//        IDataSet dataSet = new XmlDataSet(new FileReader(new File("src/test/resources/xml/dataSetTest.xml")));

        DiffCollectingFailureHandler myHandler = new DiffCollectingFailureHandler();
        final FailureHandler failureHandler = myHandler;

        assertion.assertEquals(dataSet.getTable("TEST_TABLE"), dataSet.getTable("TEST_TABLE_WITH_WRONG_VALUE"),
                failureHandler, c -> false);

        List<DbComparisonFailure> diffList = myHandler.getErrors();
        assertEquals(1, diffList.size());
        DbComparisonFailure diff = diffList.get(0);
        assertEquals("value (table=TEST_TABLE, row=1, col=COLUMN2)", diff.getReason());
        assertEquals("row 1 col 2", diff.getExpected());
        assertEquals("wrong value", diff.getActual());
    }
}
