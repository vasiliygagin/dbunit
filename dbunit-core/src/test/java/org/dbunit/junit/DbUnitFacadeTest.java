/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.model.FrameworkMethod;

/**
 *
 */
public class DbUnitFacadeTest {

    private static List<String> events = new ArrayList<>();

    @Rule
    public DbUnitFacade dbUnit = new DbUnitFacade() {

        @Override
        protected void before(Object target, FrameworkMethod method) throws Throwable {
            events.add("Before");
        }

        @Override
        protected void after() {
            events.add("After");
        }
    };

    @Test
    public void test() {
        events.add("Test");
    }

    @AfterClass
    public static void checkEvents() {
        assertEquals(Arrays.asList("Before", "Test", "After"), events);
    }
}
