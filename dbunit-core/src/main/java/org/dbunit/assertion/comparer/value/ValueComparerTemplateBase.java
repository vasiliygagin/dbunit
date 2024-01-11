package org.dbunit.assertion.comparer.value;

import org.dbunit.DatabaseUnitException;
import org.dbunit.dataset.datatype.DataType;

/**
 * Base class for {@link ValueComparer}s, providing template methods and common
 * elements.
 *
 * @author Jeff Jensen
 * @since 2.6.0
 */
public abstract class ValueComparerTemplateBase extends ValueComparerBase {
    /**
     * {@inheritDoc}
     *
     * This implementation calls
     * {@link #isExpected(DataType, Object, Object)}.
     *
     * @see ValueComparer#compare(DataType, Object, Object)
     */
    @Override
    protected String doCompare(final DataType dataType, final Object expectedValue, final Object actualValue)
            throws DatabaseUnitException {
        final String failMessage;

        final boolean isExpected = isExpected(dataType, expectedValue, actualValue);
        if (isExpected) {
            failMessage = null;
        } else {
            failMessage = makeFailMessage(expectedValue, actualValue);
        }

        return failMessage;
    }

    /**
     * Makes the fail message using {@link #getFailPhrase()}.
     *
     * @return the formatted fail message with the fail phrase.
     */
    protected String makeFailMessage(final Object expectedValue, final Object actualValue) {
        final String failPhrase = getFailPhrase();
        return String.format(BASE_FAIL_MSG, actualValue, failPhrase, expectedValue);
    }

    /** @return true if comparing actual to expected is as expected. */
    protected abstract boolean isExpected(final DataType dataType, final Object expectedValue, final Object actualValue)
            throws DatabaseUnitException;

    /** @return The text snippet for substitution in {@link #BASE_FAIL_MSG}. */
    protected abstract String getFailPhrase();
}
