/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal;

import java.util.ArrayList;
import java.util.List;

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
    private TestContext testContext;

    private List<DbunitTask> tasksBefore = new ArrayList<>();
    private List<DbunitTask> tasksAfter = new ArrayList<>();

    @Override
    public Statement apply(Statement base, FrameworkMethod method, Object target) {

        this.testContext = new TestContext();

        return new Statement() {

            @Override
            public void evaluate() throws Throwable {
                try {
                    context.getAnnotationProcessor().configureTest(target.getClass(), method.getMethod(), testContext,
                            DbUnitRule.this);
                } catch (DatabaseException exc) {
                    throw new AssertionError("Unable to configure test", exc);
                }

                before(target, method);
                try {
                    base.evaluate();
                } finally {
                    after();
                    rollbackConnections();
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
        for (DbunitTask task : tasksBefore) {
            task.execute(testContext);
        }
    }

    /**
     * Override to tear down your specific external resource.
     * @param testContext
     * @throws Throwable
     */
    protected void after() throws Throwable {
        for (DbunitTask task : tasksAfter) {
            task.execute(testContext);
        }
    }

    protected void rollbackConnections() {
        testContext.rollbackConnections();
    }

    /**
     * Only here to make testing of dbUnit itself easier
     */
    protected void releaseTestContext() {
        testContext.releaseConnections();
        testContext = null;
    }

    public TestContext getTestContext() {
        return testContext;
    }

    public void addTaskBefore(DbunitTask task) {
        tasksBefore.add(task);
    }

    public void addTaskAfter(DbunitTask task) {
        tasksAfter.add(task);
    }
}
