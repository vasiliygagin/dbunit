/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal.annotations;

import java.lang.reflect.Method;

import org.dbunit.database.AbstractDatabaseConnection;
import org.dbunit.junit.DatabaseException;
import org.dbunit.junit.SqlAfter;
import org.dbunit.junit.SqlBefore;
import org.dbunit.junit.internal.SqlScriptExecutor;
import org.dbunit.junit.internal.TestContext;

/**
 *
 */
public class SqlAnnotationProcessor {

    public void process(Class<? extends Object> klass, Method method, TestContext testContext) {
        processBeforeAnnotations(testContext, klass.getAnnotationsByType(SqlBefore.class));
        processBeforeAnnotations(testContext, method.getAnnotationsByType(SqlBefore.class));
        processAfterAnnotations(testContext, klass.getAnnotationsByType(SqlAfter.class));
        processAfterAnnotations(testContext, method.getAnnotationsByType(SqlAfter.class));
    }

    void processBeforeAnnotations(TestContext testContext, SqlBefore[] annotations) {
        for (SqlBefore annotation : annotations) {
            testContext.addTaskBefore(tc -> {
                AbstractDatabaseConnection connection = selectConnection(tc, annotation.dataSourceName());
                SqlScriptExecutor.execute(connection, annotation.filePath());
            });
        }
    }

    void processAfterAnnotations(TestContext testContext, SqlAfter[] annotations) {
        for (SqlAfter annotation : annotations) {
            testContext.addTaskAfter(tc -> {
                AbstractDatabaseConnection connection = selectConnection(tc, annotation.dataSourceName());
                SqlScriptExecutor.execute(connection, annotation.filePath());
            });
        }
    }

    AbstractDatabaseConnection selectConnection(TestContext testContext, String dataSourceName)
            throws DatabaseException {
        AbstractDatabaseConnection connection;
        if (dataSourceName.isEmpty()) {
            connection = testContext.getConnection();
        } else {
            connection = testContext.getConnection(dataSourceName);
        }
        return connection;
    }
}
