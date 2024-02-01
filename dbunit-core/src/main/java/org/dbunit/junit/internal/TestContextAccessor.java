/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal;

import java.lang.reflect.Method;

import org.dbunit.junit.DatabaseException;

/**
 *
 */
public class TestContextAccessor {

    private static final ThreadLocal<TestContext> TC = new ThreadLocal<>();

    public static TestContextDriver buildTestContext() {
        TestContext testContext = TC.get();
        if (testContext == null) {
            testContext = new TestContext();
            TC.set(testContext);
            return new NewTestContext(testContext);
        } else {
            return new ExistingTestContext(testContext);
        }
    }

    static class NewTestContext implements TestContextDriver {

        private final TestContext testContext;

        public NewTestContext(TestContext testContext) {
            this.testContext = testContext;
        }

        @Override
        public TestContext getTestContext() {
            return testContext;
        }

        @Override
        public void configureTestContext(Class<?> klass, Method method) throws DatabaseException {
            testContext.configureTestContext(klass, method);
        }

        @Override
        public void beforeTest() throws Exception {
            testContext.beforeTest();
        }

        @Override
        public void afterTest() throws Exception {
            testContext.afterTest();
        }

        @Override
        public void rollbackConnections() {
            testContext.rollbackConnections();
        }

        @Override
        public void releaseTestContext() {
            testContext.releaseConnections();
            TC.remove();
        }
    }

    static class ExistingTestContext implements TestContextDriver {

        private final TestContext testContext;

        public ExistingTestContext(TestContext testContext) {
            this.testContext = testContext;
        }

        @Override
        public TestContext getTestContext() {
            return testContext;
        }

        @Override
        public void configureTestContext(Class<?> klass, Method method) throws DatabaseException {
        }

        @Override
        public void beforeTest() throws Exception {
        }

        @Override
        public void afterTest() throws Exception {
        }

        @Override
        public void rollbackConnections() {
        }

        @Override
        public void releaseTestContext() {
        }
    }
}
