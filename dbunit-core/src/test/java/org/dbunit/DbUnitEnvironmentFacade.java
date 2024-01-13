/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit;

import org.dbunit.junit.DbUnitFacade;
import org.junit.runners.model.FrameworkMethod;

/**
 *
 */
public class DbUnitEnvironmentFacade extends DbUnitFacade {

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
}
