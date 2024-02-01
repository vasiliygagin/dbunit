/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal;

import org.dbunit.junit.DatabaseException;
import org.dbunit.operation.DbunitTask;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

/**
 * JUnit 4 way of integrating DbUnit framework into JUnit
 *
 * <pre>
 * </pre>
 */
public abstract class DbUnitRule implements MethodRule {

    protected final GlobalContext context = GlobalContext.getIt();
    private TestContextDriver testContextDriver;

    @Override
    public Statement apply(Statement base, FrameworkMethod method, Object target) {

        testContextDriver = TestContextAccessor.buildTestContext();

        return new Statement() {

            @Override
            public void evaluate() throws Throwable {
                try {
                    testContextDriver.configureTestContext(target.getClass(), method.getMethod());
                } catch (DatabaseException exc) {
                    throw new AssertionError("Unable to configure test", exc);
                }

                before(target, method);
                testContextDriver.beforeTest();
                try {
                    base.evaluate();
                } finally {
                    testContextDriver.afterTest();
                    after();
                    testContextDriver.rollbackConnections();
                    releaseTestContext();
                }
            }
        };
    }

    /**
     * Override to set up your specific external resource.
     * @param target
     * @param method
     * @param testContext
     *
     * @throws Throwable if setup fails (which will disable {@code after}
     */
    protected void before(Object target, FrameworkMethod method) throws Throwable {
    }

    /**
     * Override to tear down your specific external resource.
     * @param testContext
     * @throws Throwable
     */
    protected void after() throws Throwable {
    }

    /**
     * Only here to make testing of dbUnit itself easier
     */
    protected void releaseTestContext() {
        testContextDriver.releaseTestContext();
    }

    public TestContext getTestContext() {
        return testContextDriver.getTestContext();
    }

    public void addTaskBefore(DbunitTask task) {
        getTestContext().addTaskBefore(task);
    }

    public void addTaskAfter(DbunitTask task) {
        getTestContext().addTaskAfter(task);
    }
}
