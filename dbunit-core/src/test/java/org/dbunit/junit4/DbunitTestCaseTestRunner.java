/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit4;

import java.util.ArrayList;
import java.util.List;

import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;
import org.junit.runner.Runner;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

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

    private static class AssertRule extends ExternalResource {

        private static final ThreadLocal<AssertRule> TL = new ThreadLocal<>();

        private final List<DbunitAssert> assertAfters = new ArrayList<>();

        private static void addAssertAfter(DbunitAssert callable) {
            TL.get().assertAfters.add(callable);
        }

        @Override
        protected void before() throws Throwable {
            TL.set(this);
        }

        @Override
        protected void after() {
            TL.remove();
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
