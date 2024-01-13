/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal;

import org.dbunit.internal.connections.DriverManagerConnectionSource;
import org.dbunit.internal.connections.DriverManagerConnectionsCache;
import org.dbunit.internal.connections.DriverManagerConnectionsFactory;
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
    private final DriverManagerConnectionsFactory driverManagerConnectionsFactory;
    private final DriverManagerConnectionsCache driverManagerConnectionsCache;
    private final AnnotationProcessor annotationProcessor = new AnnotationProcessor();

    private boolean reuseDB = true;

    private GlobalContext() {
        driverManagerConnectionsFactory = new DriverManagerConnectionsFactory();
        driverManagerConnectionsCache = new DriverManagerConnectionsCache(driverManagerConnectionsFactory);
    }

    public static GlobalContext getIt() {
        return IT;
    }

    void setReuseDB(boolean reuseDB) {
        this.reuseDB = reuseDB;
    }

    public DriverManagerConnectionSource getDriverManagerConnectionSource() {
        return reuseDB ? driverManagerConnectionsCache : driverManagerConnectionsFactory;
    }

    public AnnotationProcessor getAnnotationProcessor() {
        return annotationProcessor;
    }
}
