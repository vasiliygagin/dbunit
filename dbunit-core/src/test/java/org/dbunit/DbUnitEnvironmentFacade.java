/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit;

import java.sql.Connection;
import java.sql.SQLException;

import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.metadata.MetadataManager;
import org.dbunit.dataset.IDataSet;
import org.dbunit.junit.DatabaseException;
import org.dbunit.junit.DbUnitFacade;
import org.dbunit.junit.internal.SqlScriptExecutor;
import org.dbunit.operation.DatabaseOperation;
import org.junit.runners.model.FrameworkMethod;

/**
 *
 */
public class DbUnitEnvironmentFacade extends DbUnitFacade {

    private final DatabaseTestingEnvironment environment;
    private Database database;

    public DbUnitEnvironmentFacade(DatabaseTestingEnvironment environment) {
        this.environment = environment;
    }

    @Override
    protected void before(Object target, FrameworkMethod method) throws Throwable {
        CaseSensitive annotation = method.getAnnotation(CaseSensitive.class);
        if (annotation != null) {
            if (!(target instanceof AbstractDatabaseTest)) {
                throw new UnsupportedOperationException(
                        "CaseSensitive annotation is only supported on AbstractDatabaseTest");
            }
            ((AbstractDatabaseTest) target).addCustomizer(config -> {
                config.setCaseSensitiveTableNames(annotation.value());
                config.setEscapePattern("\"");
            });
        }
    }

    public void setDatabase(Database database) {
        this.database = database;
    }

    @Override
    public void executeOperation(DatabaseOperation operation, IDataSet dataSet)
            throws DatabaseUnitException, SQLException {
        Connection jdbcConnection = database.getJdbcConnection();
        String schema = environment.getSchema();
        MetadataManager metadataManager = new MetadataManager(jdbcConnection, database.databaseConfig, null, schema);
        DatabaseConnection connection = new DatabaseConnection(jdbcConnection, database.databaseConfig, schema,
                metadataManager);
        operation.execute(connection, dataSet);
    }

    /**
     * @param string
     * @throws DatabaseException
     */
    public void executeSqlScript(String filePath) throws DatabaseException {
        SqlScriptExecutor.execute(database.getConnection(), filePath);
    }
}
