package org.dbunit.assertion.comparer.value;

import org.dbunit.DatabaseUnitException;
import org.dbunit.dataset.datatype.DataType;

/**
 * {@link ValueComparer} implementation that verifies actual value is less than
 * or equal to expected value.
 *
 * @author Jeff Jensen
 * @since 2.6.0
 */
public class IsActualLessThanOrEqualToExpectedValueComparer extends ValueComparerBase {
    @Override
    protected boolean isExpected(final DataType dataType, final Object expectedValue, final Object actualValue)
            throws DatabaseUnitException {
        return dataType.compare(actualValue, expectedValue) < 1;
    }

    @Override
    protected String getFailPhrase() {
        return "not less than or equal to";
    }
}
