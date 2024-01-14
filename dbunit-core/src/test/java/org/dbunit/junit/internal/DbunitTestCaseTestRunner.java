/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal;

import java.util.ArrayList;
import java.util.List;

import org.dbunit.junit4.DbunitAssert;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

/**
 * Junit {@link Runner} intended to test DBunit Junit code.
 */
public class DbunitTestCaseTestRunner extends BlockJUnit4ClassRunner {

    /**
     * @param klass
     * @throws InitializationError
     */
    public DbunitTestCaseTestRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }

    @Override
    protected List<TestRule> getTestRules(Object target) {
        List<TestRule> testRules = super.getTestRules(target);
        testRules.add(new AssertRule());
        return testRules;
    }

    public static void assertAfter(DbunitAssert callable) {
        AssertRule.addAssertAfter(callable);
    }

    private static class AssertRule implements TestRule {

        private static final ThreadLocal<AssertRule> TL = new ThreadLocal<>();

        private final List<DbunitAssert> assertAfters = new ArrayList<>();

        private static void addAssertAfter(DbunitAssert callable) {
            TL.get().assertAfters.add(callable);
        }

        @Override
        public Statement apply(Statement base, Description description) {
            return statement(base);
        }

        private Statement statement(final Statement base) {
            return new Statement() {

                @Override
                public void evaluate() throws Throwable {
                    TL.set(AssertRule.this);
                    GlobalContext.getIt().setReuseDB(false);

                    try {
                        before();
                        base.evaluate();
                        after();
                    } finally {
                        TL.remove();
                        GlobalContext.getIt().setReuseDB(true);

                        runAssertAfters();
                    }
                }
            };
        }

        /**
         * Override to set up your specific external resource.
         *
         * @throws Throwable if setup fails (which will disable {@code after}
         */
        protected void before() throws Throwable {
        }

        /**
         * Override to tear down your specific external resource.
         */
        protected void after() {
        }

        void runAssertAfters() throws AssertionError {
            for (DbunitAssert assertAfter : assertAfters) {
                try {
                    assertAfter.execute();
                } catch (Throwable exc) {
                    throw new AssertionError("After test assert failed", exc);
                }
            }
        }
    }
}
