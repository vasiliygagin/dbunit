/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal.annotations;

import java.lang.reflect.Method;

import org.dbunit.junit.DatabaseException;
import org.dbunit.junit.internal.DbUnitRule;
import org.dbunit.junit.internal.TestContext;

/**
 * This class responsible for processing annotations on test class
 */
public class AnnotationProcessor {

    private DriverManagerConnectionAnnotationProcessor driverManagerConnectionAnnotationProcessor = new DriverManagerConnectionAnnotationProcessor();
    private PropertiesDriverManagerConnectionAnnotationProcessor propertiesDriverManagerConnectionAnnotationProcessor = new PropertiesDriverManagerConnectionAnnotationProcessor();
    private SchemaAnnotationProcessor schemaAnnotationProcessor = new SchemaAnnotationProcessor();
    private DataSourceAnnotationProcessor dataSourceAnnotationProcessor = new DataSourceAnnotationProcessor();
    private SqlAnnotationProcessor sqlAnnotationProcessor = new SqlAnnotationProcessor();

    /**
     * @param klass
     * @param method
     * @param dbUnitRule
     * @throws DatabaseException
     */
    public void configureTest(Class<? extends Object> klass, Method method, TestContext testContext,
            DbUnitRule dbUnitRule) throws DatabaseException {
        driverManagerConnectionAnnotationProcessor.process(klass, testContext);
        propertiesDriverManagerConnectionAnnotationProcessor.process(klass, testContext);
        schemaAnnotationProcessor.process(klass, testContext);
        dataSourceAnnotationProcessor.process(klass, testContext);
        sqlAnnotationProcessor.process(klass, method, dbUnitRule);
    }
}
