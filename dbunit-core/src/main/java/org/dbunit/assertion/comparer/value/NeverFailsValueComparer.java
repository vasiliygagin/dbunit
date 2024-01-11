package org.dbunit.assertion.comparer.value;

import org.dbunit.DatabaseUnitException;
import org.dbunit.dataset.datatype.DataType;

/**
 * {@link ValueComparer} implementation that verifies nothing and never fails;
 * {@link #isExpected(DataType, Object, Object)}
 * always returns true.
 *
 * @author Jeff Jensen
 * @since 2.6.0
 */
public class NeverFailsValueComparer extends ValueComparerBase {
    @Override
    protected boolean isExpected(final DataType dataType, final Object expectedValue, final Object actualValue)
            throws DatabaseUnitException {
        return true;
    }

    @Override
    protected String getFailPhrase() {
        return "";
    }
}
