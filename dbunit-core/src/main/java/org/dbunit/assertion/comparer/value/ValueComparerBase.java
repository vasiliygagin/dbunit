package org.dbunit.assertion.comparer.value;

import org.dbunit.DatabaseUnitException;
import org.dbunit.dataset.datatype.DataType;

/**
 * Base class for {@link ValueComparer}s providing a template method and common
 * elements, mainly consistent log message and toString.
 *
 * @author Jeff Jensen
 * @since 2.6.0
 */
public abstract class ValueComparerBase implements ValueComparer {

    /**
     * Format String for consistent fail message; substitution strings are: actual,
     * fail phrase, expected.
     */
    public static final String BASE_FAIL_MSG = "Actual value='%s' is %s expected value='%s'";

    /**
     * {@inheritDoc}
     *
     * This implementation calls
     * {@link #doCompare(DataType, Object, Object)}.
     */
    @Override
    public String compare(final DataType dataType, final Object expectedValue, final Object actualValue)
            throws DatabaseUnitException {
        final String failMessage;

        failMessage = doCompare(dataType, expectedValue, actualValue);

        return failMessage;
    }

    /**
     * Do the comparison and return a fail message or null if comparison passes.
     *
     * @see ValueComparer#compare(DataType, Object, Object)
     */
    protected abstract String doCompare(final DataType dataType, final Object expectedValue, final Object actualValue)
            throws DatabaseUnitException;
}
