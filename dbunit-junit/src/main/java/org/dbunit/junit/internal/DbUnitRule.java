/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal;

import org.dbunit.junit.DatabaseException;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

/**
 * JUnit 4 way of integrating DbUnit framework into JUnit
 *
 * <pre>
 * </pre>
 */
public class DbUnitRule implements MethodRule {

    protected final GlobalContext context = GlobalContext.getIt();
    private TestContext testContext;

    @Override
    public Statement apply(Statement base, FrameworkMethod method, Object target) {

        this.testContext = new TestContext();

        try {
            context.getAnnotationProcessor().configureTest(target.getClass(), testContext);
        } catch (DatabaseException exc) {
            throw new AssertionError("Unable to configure test", exc);
        }

        return new Statement() {

            @Override
            public void evaluate() throws Throwable {
                before();
                try {
                    base.evaluate();
                } finally {
                    after();
                    testContext = null;
                }
            }
        };
    }

    /**
     * Override to set up your specific external resource.
     * @param testContext
     *
     * @throws Throwable if setup fails (which will disable {@code after}
     */
    protected void before() throws Throwable {
    }

    /**
     * Override to tear down your specific external resource.
     * @param testContext
     */
    protected void after() {
    }

    public TestContext getTestContext() {
        return testContext;
    }

}
