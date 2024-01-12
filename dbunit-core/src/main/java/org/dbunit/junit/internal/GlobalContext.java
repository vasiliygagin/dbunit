/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal;

import org.dbunit.junit.internal.annotations.AnnotationProcessor;
import org.dbunit.junit.internal.connections.DatabaseConnectionManager;

/**
 * Shared dbUnit context. There is only one instance of it.
 *
 * VG. This might move to dbunit-core
 */
public class GlobalContext {

    private static GlobalContext IT = new GlobalContext();
    {
        IT = this;
    }
    private final DatabaseConnectionManager dbConnectionManager = new DatabaseConnectionManager();
    private final AnnotationProcessor annotationProcessor = new AnnotationProcessor();

    private GlobalContext() {
    }

    public static GlobalContext getIt() {
        return IT;
    }

    public DatabaseConnectionManager getDbConnectionManager() {
        return dbConnectionManager;
    }

    public AnnotationProcessor getAnnotationProcessor() {
        return annotationProcessor;
    }
}
