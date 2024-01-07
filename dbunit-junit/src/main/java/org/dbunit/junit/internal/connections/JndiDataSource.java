/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal.connections;

import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.dbunit.assertion.DefaultFailureHandler;
import org.dbunit.assertion.SimpleAssert;

/**
 *
 */
public class JndiDataSource extends DataSourceProxy {

    public JndiDataSource(String lookupName, Properties environment) {
        super(lookupDataSource(lookupName, environment));
    }

    private static DataSource lookupDataSource(String lookupName, Properties environment) {
        SimpleAssert simpleAssert = new SimpleAssert(new DefaultFailureHandler());
        simpleAssert.assertTrue("lookupName" + " is null", lookupName != null);
        simpleAssert.assertTrue("Invalid " + "lookupName", lookupName.trim().length() > 0);
        try {
            Context context = new InitialContext(environment);
            Object obj = context.lookup(lookupName);
            simpleAssert.assertTrue("JNDI object with [" + lookupName + "] not found", obj != null);
            simpleAssert.assertTrue("Object [" + obj + "] at JNDI location [" + lookupName + "] is not of type ["
                    + DataSource.class.getName() + "]", obj instanceof DataSource);
            return (DataSource) obj;
        } catch (NamingException exc) {
            throw new AssertionError("Unable to lookup JNDI datasource [" + lookupName + "]", exc);
        }
    }
}
