package org.dbunit;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 * Strategy pattern for verifying {@link VerifyTableDefinition}s and
 * expectedDataSet configurations agree, e.g. have the same number of tables
 * defined.
 *
 * @author Jeff Jensen
 */
public interface ExpectedDataSetAndVerifyTableDefinitionVerifier {

    /**
     * Verify {@link VerifyTableDefinition}s and expectedDataSet configurations
     * agree.
     */
    void verify(VerifyTableDefinition[] verifyTableDefinitions, IDataSet expectedDataSet, DatabaseConfig config)
            throws DataSetException;
}
