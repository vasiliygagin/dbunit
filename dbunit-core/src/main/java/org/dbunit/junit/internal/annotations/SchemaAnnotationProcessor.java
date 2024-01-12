/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal.annotations;

import org.dbunit.junit.Schema;
import org.dbunit.junit.internal.GlobalContext;
import org.dbunit.junit.internal.TestContext;

/**
 *
 */
class SchemaAnnotationProcessor {

    private static final GlobalContext context = GlobalContext.getIt();

    public void process(Class<? extends Object> klass, TestContext testContext) {

        Schema annotation = klass.getAnnotation(Schema.class);
        if (annotation != null) {
            testContext.setSchema(annotation.value());
        }
    }
}
