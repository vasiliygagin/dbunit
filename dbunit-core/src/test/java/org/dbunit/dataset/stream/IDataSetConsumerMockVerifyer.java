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
package org.dbunit.dataset.stream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Iterator;

import org.dbunit.dataset.ITableMetaData;
import org.hamcrest.Matchers;
import org.mockito.MockingDetails;
import org.mockito.Mockito;
import org.mockito.invocation.Invocation;

public class IDataSetConsumerMockVerifyer {

    private static final Method startDataSet;
    private static final Method startTable;
    private static final Method row;
    private static final Method endTable;
    private static final Method endDataSet;
    static {
	try {
	    startDataSet = IDataSetConsumer.class.getMethod("startDataSet");
	    startTable = IDataSetConsumer.class.getMethod("startTable", ITableMetaData.class);
	    row = IDataSetConsumer.class.getMethod("row", Object[].class);
	    endTable = IDataSetConsumer.class.getMethod("endTable");
	    endDataSet = IDataSetConsumer.class.getMethod("endDataSet");
	} catch (Exception exc) {
	    throw new IllegalStateException(exc);
	}
    }

    private final Iterator<Invocation> invocationIterator;

    public IDataSetConsumerMockVerifyer(IDataSetConsumer mock) {
	MockingDetails mockingDetails = Mockito.mockingDetails(mock);
	Collection<Invocation> invocations = mockingDetails.getInvocations();
	invocationIterator = invocations.iterator();
    }

    private Invocation nextInvocation() {
	if (!invocationIterator.hasNext()) {
	    throw new AssertionError("No more interaction recorded");
	}
	return invocationIterator.next();
    }

    public void verifyStartDataSet() {
	Invocation invocation = nextInvocation();
	assertThat(invocation.getMethod(), is(startDataSet));
    }

    public void verifyStartTable(ITableMetaData tableMetaData) {
	Invocation invocation = nextInvocation();
	assertThat(invocation.getMethod(), is(startTable));
	assertThat(invocation.getArgument(0), is(tableMetaData));
    }

    public void verifyRow(Object... columns) {
	Invocation invocation = nextInvocation();
	assertThat(invocation.getMethod(), is(row));
	assertThat(invocation.getArgument(0), Matchers.arrayContaining(columns));
    }

    public void verifyEndTable() {
	Invocation invocation = nextInvocation();
	assertThat(invocation.getMethod(), is(endTable));
    }

    public void verifyEndDataSet() {
	Invocation invocation = nextInvocation();
	assertThat(invocation.getMethod(), is(endDataSet));
    }

    public void verifyNoMoreInvocations() {
	if (invocationIterator.hasNext()) {
	    dumpRest();
	    throw new AssertionError("More interaction recorded");
	}
    }

    public void dumpRest() {
	while (invocationIterator.hasNext()) {
	    Invocation invocation = invocationIterator.next();
	    System.out.println(invocation.getMethod().getName() + " = " + invocation.getArguments());
	}
    }
}
