/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit;

import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

// TODO needs to be renamed to IT
public abstract class AbstractDatabaseTest {

    protected final DatabaseTestingEnvironment environment;
    protected Database database;
    private boolean environmentOk;

    @Rule
    public final DbUnitEnvironmentFacade dbUnit;

    private final List<Consumer<DatabaseConfig>> configCustomizers = new ArrayList<>();

    public AbstractDatabaseTest() throws Exception {
        environment = DatabaseEnvironmentLoader.getInstance();
        dbUnit = new DbUnitEnvironmentFacade(environment);
    }

    @Before
    public final void openDatabase() throws Exception {
        environmentOk = checkEnvironment();
        assumeTrue(environmentOk);
        database = doOpenDatabase();
        dbUnit.setDatabase(database);
    }

    /**
     * @return <code>true</code> if environment is ok to run tests. Which is default. Otherwise tests are ignored
     */
    protected boolean checkEnvironment() {
        return true;
    }

    /**
     * Chance to overwrite db
     */
    protected Database doOpenDatabase() throws Exception {
        return environment.openPopulatedDatabase(getconfigCustomizers());
    }

    protected Consumer[] getconfigCustomizers() {
        return configCustomizers.toArray(new Consumer[configCustomizers.size()]);
    }

    /**
     * If environment is not Ok, then Befores did not run. And presumably Afters do not need to run either. Though JUnit is running them.
     * This method gives you an opportunity to check inside After if it should be executed or not.
     * @return
     */
    public boolean isEnvironmentOk() {
        return environmentOk;
    }

    @After
    public final void closeDatabase() {
        if (environmentOk) {
            environment.closeDatabase(database);
        }
    }

    /**
     * @param object
     */
    public void addCustomizer(Consumer<DatabaseConfig> configCustomizer) {
        configCustomizers.add(configCustomizer);
    }

    protected FileReader fileReader(String fileName) throws FileNotFoundException {
        return new FileReader(new File(fileName).getAbsoluteFile());
    }
}
