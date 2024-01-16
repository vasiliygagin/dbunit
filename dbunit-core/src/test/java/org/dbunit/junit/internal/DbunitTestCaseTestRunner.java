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

    private static ThreadLocal<OuterStatement> TL = new ThreadLocal<>();

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
        testRules.add(new OuterStatementMakerRule(target));
        return testRules;
    }

    public static void assertAfter(DbunitAssert callable) {
        TL.get().addAssertAfter(callable);
    }

    private class OuterStatementMakerRule implements TestRule {

        private Object testInstance;

        public OuterStatementMakerRule(Object testInstance) {
            this.testInstance = testInstance;
        }

        @Override
        public Statement apply(Statement base, Description description) {
            return new OuterStatement(base, testInstance);
        }
    }

    private class OuterStatement extends Statement {

        private final Object testInstance;
        private final Statement innerStataement;
        private final List<DbunitAssert> assertAfters = new ArrayList<>();

        public OuterStatement(Statement innerStataement, Object testInstance) {
            this.testInstance = testInstance;
            this.innerStataement = innerStataement;
        }

        private void addAssertAfter(DbunitAssert callable) {
            assertAfters.add(callable);
        }

        @Override
        public void evaluate() throws Throwable {
            TL.set(this);
            GlobalContext.getIt().setReuseDB(false);

            try {
                beforeTest();

                innerStataement.evaluate();

                afterTest();
            } finally {
                TL.remove();
                GlobalContext.getIt().setReuseDB(true);

                runAssertAfters();
            }
        }

        private void beforeTest() throws Exception {
            if (testInstance instanceof InternalTestCase) {
                ((InternalTestCase) testInstance).beforeTestCase();
            }
        }

        private void afterTest() throws Exception {
            if (testInstance instanceof InternalTestCase) {
                ((InternalTestCase) testInstance).afterTestCase();
            }
        }

        private void runAssertAfters() throws AssertionError {
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
