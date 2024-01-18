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

package org.dbunit;

import java.sql.Connection;

import org.dbunit.assertion.DefaultFailureHandler;
import org.dbunit.assertion.SimpleAssert;
import org.dbunit.database.AbstractDatabaseConnection;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.metadata.MetadataManager;
import org.dbunit.dataset.IDataSet;
import org.dbunit.operation.DatabaseOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 * Basic implementation of IDatabaseTester.<br>
 * Implementations of IDatabaseTester may use this class as a starting point.
 *
 * @author Andres Almiray (aalmiray@users.sourceforge.net)
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since 2.2.0
 */
public abstract class AbstractDatabaseTester extends SimpleAssert implements IDatabaseTester {

    /**
     * Logger for this class
     */
    private static final Logger logger = LoggerFactory.getLogger(AbstractDatabaseTester.class);

    /**
     * Enumeration of the valid {@link OperationType}s
     */
    static final class OperationType {

        public static final OperationType SET_UP = new OperationType("setUp");
        public static final OperationType TEAR_DOWN = new OperationType("tearDown");

        private final String key;

        private OperationType(String key) {
            this.key = key;
        }

        @Override
        public String toString() {
            return "OperationType: " + key;
        }
    }

    private IDataSet dataSet;
    private String schema;
    private DatabaseOperation setUpOperation = DatabaseOperation.CLEAN_INSERT;
    private DatabaseOperation tearDownOperation = DatabaseOperation.NONE;
    private IOperationListener operationListener;
    private DatabaseConfig databaseConfig = new DatabaseConfig();

    public AbstractDatabaseTester() {
        this(null);
    }

    /**
     * @param schema The schema to be tested. Can be <code>null</code>
     * @since 2.4.3
     */
    public AbstractDatabaseTester(String schema) {
        super(new DefaultFailureHandler());
        this.schema = schema;
    }

    public void setDatabaseConfig(DatabaseConfig databaseConfig) {
        this.databaseConfig = databaseConfig;
    }

    @Override
    public AbstractDatabaseConnection getConnection() throws Exception {
        Connection connection = buildJdbcConnection();
        return buildConnection(databaseConfig, connection);
    }

    protected abstract Connection buildJdbcConnection() throws Exception;

    protected final DatabaseConnection buildConnection(DatabaseConfig config, Connection conn)
            throws DatabaseUnitException {
        MetadataManager metadataManager = new MetadataManager(conn, config, null, getSchema());
        return new DatabaseConnection(conn, config, getSchema(), metadataManager);
    }

    @Override
    public void closeConnection(IDatabaseConnection connection) throws Exception {
        logger.debug("closeConnection(connection={}) - start", connection);

        connection.close();
    }

    @Override
    public IDataSet getDataSet() {
        logger.debug("getDataSet() - start");

        return dataSet;
    }

    @Override
    public void onSetup() throws Exception {
        logger.debug("onSetup() - start");
        executeOperation(getSetUpOperation(), OperationType.SET_UP);
    }

    @Override
    public void onTearDown() throws Exception {
        logger.debug("onTearDown() - start");
        executeOperation(getTearDownOperation(), OperationType.TEAR_DOWN);
    }

    @Override
    public void setDataSet(IDataSet dataSet) {
        logger.debug("setDataSet(dataSet={}) - start", dataSet);

        this.dataSet = dataSet;
    }

    @Override
    public void setSchema(String schema) {
        logger.debug("setSchema(schema={}) - start", schema);

        logger.warn("setSchema() should not be used anymore");
        this.schema = schema;
    }

    @Override
    public void setSetUpOperation(DatabaseOperation setUpOperation) {
        logger.debug("setSetUpOperation(setUpOperation={}) - start", setUpOperation);

        this.setUpOperation = setUpOperation;
    }

    @Override
    public void setTearDownOperation(DatabaseOperation tearDownOperation) {
        logger.debug("setTearDownOperation(tearDownOperation={}) - start", tearDownOperation);

        this.tearDownOperation = tearDownOperation;
    }

    /**
     * Returns the schema value.
     */
    protected String getSchema() {
        logger.trace("getSchema() - start");

        return schema;
    }

    /**
     * Returns the DatabaseOperation to call when starting the test.
     */
    @Override
    public DatabaseOperation getSetUpOperation() {
        logger.trace("getSetUpOperation() - start");

        return setUpOperation;
    }

    /**
     * Returns the DatabaseOperation to call when ending the test.
     */
    @Override
    public DatabaseOperation getTearDownOperation() {
        logger.trace("getTearDownOperation() - start");

        return tearDownOperation;
    }

    /**
     * Executes a DatabaseOperation with a IDatabaseConnection supplied by
     * {@link #getConnection()} and the test dataset.
     */
    private void executeOperation(DatabaseOperation operation, OperationType type) throws Exception {
        logger.debug("executeOperation(operation={}) - start", operation);

        if (operation != DatabaseOperation.NONE) {
            // Ensure that the operationListener is set
            if (operationListener == null) {
                logger.debug("OperationListener is null and will be defaulted.");
                operationListener = new DefaultOperationListener();
            }

            AbstractDatabaseConnection connection = getConnection();
            operationListener.connectionRetrieved(connection);

            try {
                operation.execute(connection, getDataSet());
            } finally {
                // Since 2.4.4 the OperationListener is responsible for closing
                // the connection at the right time
                if (type == OperationType.SET_UP) {
                    operationListener.operationSetUpFinished(connection);
                } else if (type == OperationType.TEAR_DOWN) {
                    operationListener.operationTearDownFinished(connection);
                } else {
                    throw new DatabaseUnitRuntimeException("Cannot happen - unknown OperationType specified: " + type);
                }
            }
        }
    }

    @Override
    public void setOperationListener(IOperationListener operationListener) {
        logger.debug("setOperationListener(operationListener={}) - start", operationListener);
        this.operationListener = operationListener;
    }
}
