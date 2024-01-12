/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal;

import org.dbunit.junit.internal.annotations.AnnotationProcessor;

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
    private final AnnotationProcessor annotationProcessor = new AnnotationProcessor();

    private GlobalContext() {
    }

    public static GlobalContext getIt() {
        return IT;
    }

    public AnnotationProcessor getAnnotationProcessor() {
        return annotationProcessor;
    }
}
