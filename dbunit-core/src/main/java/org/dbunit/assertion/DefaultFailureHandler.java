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

import org.dbunit.dataset.Column;
import org.dbunit.dataset.Columns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the {@link FailureHandler}.
 *
 * @author gommma (gommma AT users.sourceforge.net)
 * @since 2.4.0
 */
public class DefaultFailureHandler implements FailureHandler {

    static final Logger logger = LoggerFactory.getLogger(DefaultFailureHandler.class);

    private FailureHandler failureFactory = new DefaultFailureFactory();
    private MessageBuilder messageBuilder;

    /**
     * Default constructor which does not provide any additional column information.
     */
    public DefaultFailureHandler() {
        messageBuilder = new MessageBuilder();
    }

    public MessageBuilder getMessageBuilder() {
        return messageBuilder;
    }

    /**
     * Create a default failure handler
     *
     * @param additionalColumnInfo the column names of the columns for which
     *                             additional information should be printed when an
     *                             assertion failed.
     */
    public DefaultFailureHandler(final Column[] additionalColumnInfo) {

        messageBuilder = new MessageBuilder(
                additionalColumnInfo == null ? null : Columns.getColumnNames(additionalColumnInfo));
    }

    /**
     * Create a default failure handler
     *
     * @param additionalColumnInfo the column names of the columns for which
     *                             additional information should be printed when an
     *                             assertion failed.
     */
    public DefaultFailureHandler(final String[] additionalColumnInfo) {
        messageBuilder = new MessageBuilder(additionalColumnInfo);
    }

    /**
     * @param failureFactory The {@link FailureFactory} to be used for creating
     *                       assertion errors.
     */
    public void setFailureFactory(final FailureHandler failureFactory) {
        if (failureFactory == null) {
            throw new NullPointerException("The parameter 'failureFactory' must not be null");
        }
        this.failureFactory = failureFactory;
    }

    @Override
    public void handleFailure(final String message, final String expected, final String actual) {
        this.failureFactory.handleFailure(message, expected, actual);
    }

    @Override
    public void handleFailure(final String message) {
        this.failureFactory.handleFailure(message);
    }

    /**
     * Default failure factory which returns DBUnits own assertion error instances.
     *
     * @author gommma (gommma AT users.sourceforge.net)
     * @author Last changed by: $Author: gommma $
     * @version $Revision: 872 $ $Date: 2008-11-08 09:45:52 -0600 (Sat, 08 Nov 2008)
     *          $
     * @since 2.4.0
     */
    public static class DefaultFailureFactory implements FailureHandler {

        @Override
        public void handleFailure(final String message, final String expected, final String actual) {
            // Return dbunit's own comparison failure object
            throw new DbComparisonFailure(message, expected, actual);
        }

        @Override
        public void handleFailure(final String message) {
            // Return dbunit's own failure object
            throw new DbAssertionFailedError(message);
        }
    }
}
