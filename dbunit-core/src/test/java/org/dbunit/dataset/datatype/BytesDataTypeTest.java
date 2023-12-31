/*
 *
 * The DbUnit Database Testing Framework
 * Copyright (C)2002-2004, DbUnit.org
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

package org.dbunit.dataset.datatype;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.sql.Types;
import java.util.Arrays;

import org.dbunit.database.ExtendedMockSingleRowResultSet;
import org.dbunit.database.statement.MockPreparedStatement;
import org.dbunit.dataset.ITable;
import org.dbunit.testutil.FileAsserts;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 */
public class BytesDataTypeTest extends AbstractDataTypeTest {
    private final static DataType[] TYPES = { DataType.BINARY, DataType.VARBINARY, DataType.LONGVARBINARY,
//        DataType.BLOB,
    };

    public BytesDataTypeTest(String name) {
	super(name);
    }

    @Override
    public void testToString() throws Exception {
	String[] expected = { "BINARY", "VARBINARY", "LONGVARBINARY",
//            "BLOB",
	};

	assertEquals("type count", expected.length, TYPES.length);
	for (int i = 0; i < TYPES.length; i++) {
	    assertEquals("name", expected[i], TYPES[i].toString());
	}
    }

    @Override
    public void testGetTypeClass() throws Exception {
	for (DataType element : TYPES) {
	    assertEquals("class", byte[].class, element.getTypeClass());
	}
    }

    @Override
    public void testIsNumber() throws Exception {
	for (DataType element : TYPES) {
	    assertEquals("is number", false, element.isNumber());
	}
    }

    @Override
    public void testIsDateTime() throws Exception {
	for (DataType element : TYPES) {
	    assertEquals("is date/time", false, element.isDateTime());
	}
    }

    @Override
    public void testTypeCast() throws Exception {
	Object[] values = { null, "", "YWJjZA==", new byte[] { 0, 1, 2, 3, 4, 5 },
		"[text]This is text with UTF-8 (the default) characters >>àéç<<",
		"[text UTF-8]This is text with UTF-8 (the default) characters >>àéç<<",
		"[text]c27ccbf5-6ca1-4bdd-8cb0-bacfea6a5a8b", "[base64]VGhpcyBpcyBhIHRlc3QgZm9yIGJhc2U2NC4K==" };

	byte[][] expected = { null, new byte[0], new byte[] { 'a', 'b', 'c', 'd' }, new byte[] { 0, 1, 2, 3, 4, 5 },
		values[4].toString().replaceAll("\\[.*?\\]", "").getBytes("UTF-8"),
		values[5].toString().replaceAll("\\[.*?\\]", "").getBytes("UTF-8"),
		values[6].toString().replaceAll("\\[.*?\\]", "").getBytes("UTF-8"),
		"This is a test for base64.\n".getBytes(), };

	assertEquals("actual vs expected count", values.length, expected.length);

	for (DataType element : TYPES) {
	    for (int j = 0; j < values.length; j++) {
		byte[] actual = (byte[]) element.typeCast(values[j]);
		assertTrue("typecast " + j, Arrays.equals(expected[j], actual));
	    }
	}
    }

    public void testTypeCastFileName() throws Exception {
	File file = new File("LICENSE");

	Object[] values = { "[file]" + file.toString(), file.toString(), file.getAbsolutePath(),
		file.toURI().toURL().toString(), file, file.toURI().toURL(), "[url]" + file.toURI().toURL(), };

	assertEquals("exists", true, file.exists());

	for (DataType element : TYPES) {
	    for (Object value : values) {
		byte[] actual = (byte[]) element.typeCast(value);
		FileAsserts.assertEquals(new ByteArrayInputStream(actual), file);
	    }
	}
    }

    @Override
    public void testTypeCastNone() throws Exception {
	for (DataType type : TYPES) {
	    assertEquals("typecast " + type, null, type.typeCast(ITable.NO_VALUE));
	}
    }

    @Override
    public void testTypeCastInvalid() throws Exception {
	Object[] values = { new Object(), new Integer(1234), };

	for (DataType element : TYPES) {
	    for (Object value : values) {
		try {
		    element.typeCast(value);
		    fail("Should throw TypeCastException: " + value);
		} catch (TypeCastException e) {
		}
	    }
	}
    }

    @Override
    public void testCompareEquals() throws Exception {
	Object[] values1 = { null, "", "YWJjZA==", new byte[] { 0, 1, 2, 3, 4, 5 }, };

	byte[][] values2 = { null, new byte[0], new byte[] { 'a', 'b', 'c', 'd' }, new byte[] { 0, 1, 2, 3, 4, 5 }, };

	assertEquals("values count", values1.length, values2.length);

	for (DataType element : TYPES) {
	    for (int j = 0; j < values1.length; j++) {
		assertEquals("compare1 " + j, 0, element.compare(values1[j], values2[j]));
		assertEquals("compare2 " + j, 0, element.compare(values2[j], values1[j]));
	    }
	}
    }

    @Override
    public void testCompareInvalid() throws Exception {
	Object[] values1 = { new Object(), new java.util.Date() };
	Object[] values2 = { null, null };

	assertEquals("values count", values1.length, values2.length);

	for (DataType element : TYPES) {
	    for (int j = 0; j < values1.length; j++) {
		try {
		    element.compare(values1[j], values2[j]);
		    fail("Should throw TypeCastException");
		} catch (TypeCastException e) {
		}

		try {
		    element.compare(values2[j], values1[j]);
		    fail("Should throw TypeCastException");
		} catch (TypeCastException e) {
		}
	    }
	}
    }

    @Override
    public void testCompareDifferent() throws Exception {
	Object[] less = { null, new byte[] { 'a', 'a', 'c', 'd' }, new byte[] { 0, 1, 2, 3, 4, 5 }, };
	Object[] greater = { new byte[0], new byte[] { 'a', 'b', 'c', 'd' }, new byte[] { 0, 1, 2, 3, 4, 5, 6 }, };

	assertEquals("values count", less.length, greater.length);

	for (DataType element : TYPES) {
	    for (int j = 0; j < less.length; j++) {
		assertTrue("less " + j, element.compare(less[j], greater[j]) < 0);
		assertTrue("greater " + j, element.compare(greater[j], less[j]) > 0);
	    }
	}
    }

    @Override
    public void testSqlType() throws Exception {
	int[] sqlTypes = { Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY,
//            Types.BLOB,
	};

	assertEquals("count", sqlTypes.length, TYPES.length);
	for (int i = 0; i < TYPES.length; i++) {
	    assertEquals("forSqlType", TYPES[i], DataType.forSqlType(sqlTypes[i]));
	    assertEquals("forSqlTypeName", TYPES[i], DataType.forSqlTypeName(TYPES[i].toString()));
	    assertEquals("getSqlType", sqlTypes[i], TYPES[i].getSqlType());
	}
    }

    @Override
    public void testForObject() throws Exception {
	assertEquals(DataType.VARBINARY, DataType.forObject(new byte[0]));
    }

    @Override
    public void testAsString() throws Exception {
	byte[][] values = { new byte[0], new byte[] { 'a', 'b', 'c', 'd' }, };

	String[] expected = { "", "YWJjZA==", };

	assertEquals("actual vs expected count", values.length, expected.length);

	for (int i = 0; i < values.length; i++) {
	    assertEquals("asString " + i, expected[i], DataType.asString(values[i]));
	}
    }

    @Override
    public void testGetSqlValue() throws Exception {
	byte[][] expected = { null, new byte[0], new byte[] { 'a', 'b', 'c', 'd' }, new byte[] { 0, 1, 2, 3, 4, 5 }, };

	ExtendedMockSingleRowResultSet resultSet = new ExtendedMockSingleRowResultSet();
	resultSet.addExpectedIndexedValues(expected);

	for (int i = 0; i < expected.length; i++) {
	    Object expectedValue = expected[i];

	    for (int j = 0; j < TYPES.length; j++) {
		DataType dataType = TYPES[j];
		Object actualValue = dataType.getSqlValue(i + 1, resultSet);
		assertEquals("value " + j, expectedValue, actualValue);
	    }
	}
    }

    public void testSetSqlValue() throws Exception {
	MockPreparedStatement preparedStatement = new MockPreparedStatement();

	Object[] expected = { null, new byte[0], new byte[] { 'a', 'b', 'c', 'd' }, };

	int[] expectedSqlTypesForDataType = { Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY };

	for (int i = 0; i < expected.length; i++) {
	    Object expectedValue = expected[i];

	    for (int j = 0; j < TYPES.length; j++) {
		DataType dataType = TYPES[j];
		int expectedSqlType = expectedSqlTypesForDataType[j];

		dataType.setSqlValue(expectedValue, 1, preparedStatement);
		// Check the results immediately
		assertEquals("Loop " + i + " Type " + dataType, 1, preparedStatement.getLastSetObjectParamIndex());
		assertEquals("Loop " + i + " Type " + dataType, expectedSqlType,
			preparedStatement.getLastSetObjectTargetSqlType());
		Object actualValue = preparedStatement.getLastSetObjectParamValue();
		assertEquals("Loop " + i + " Type " + dataType, expectedValue, actualValue);
	    }
	}
    }
}
