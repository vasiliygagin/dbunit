/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal.annotations;

import java.lang.reflect.Method;

import org.dbunit.database.DatabaseConnection;
import org.dbunit.junit.DatabaseException;
import org.dbunit.junit.SqlAfter;
import org.dbunit.junit.SqlBefore;
import org.dbunit.junit.internal.DbUnitRule;
import org.dbunit.junit.internal.SqlScriptExecutor;
import org.dbunit.junit.internal.TestContext;

/**
 *
 */
public class SqlAnnotationProcessor {

    /**
     * @param klass
     * @param method
     * @param dbUnitRule
     * @throws DatabaseException
     */
    public void process(Class<? extends Object> klass, Method method, DbUnitRule dbUnitRule) throws DatabaseException {
        processBeforeAnnotations(dbUnitRule, klass.getAnnotationsByType(SqlBefore.class));
        processBeforeAnnotations(dbUnitRule, method.getAnnotationsByType(SqlBefore.class));
        processAfterAnnotations(dbUnitRule, klass.getAnnotationsByType(SqlAfter.class));
        processAfterAnnotations(dbUnitRule, method.getAnnotationsByType(SqlAfter.class));
    }

    void processBeforeAnnotations(DbUnitRule dbUnitRule, SqlBefore[] annotations) {
        for (SqlBefore annotation : annotations) {
            dbUnitRule.addTaskBefore(testContext -> {
                DatabaseConnection connection = selectConnection(testContext, annotation.dataSourceName());
                SqlScriptExecutor.execute(connection, annotation.filePath());
            });
        }
    }

    void processAfterAnnotations(DbUnitRule dbUnitRule, SqlAfter[] annotations) {
        for (SqlAfter annotation : annotations) {
            dbUnitRule.addTaskAfter(testContext -> {
                DatabaseConnection connection = selectConnection(testContext, annotation.dataSourceName());
                SqlScriptExecutor.execute(connection, annotation.filePath());
            });
        }
    }

    DatabaseConnection selectConnection(TestContext testContext, String dataSourceName) throws DatabaseException {
        DatabaseConnection connection;
        if (dataSourceName.isEmpty()) {
            connection = testContext.getConnection();
        } else {
            connection = testContext.getConnection(dataSourceName);
        }
        return connection;
    }
}
