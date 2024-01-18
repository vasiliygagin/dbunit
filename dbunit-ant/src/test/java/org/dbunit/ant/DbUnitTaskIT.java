/*
 *
 * The DbUnit Database Testing Framework
 * Copyright (C)2002-2004, DbUnit.org
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

package org.dbunit.ant;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;

import org.apache.tools.ant.BuildEvent;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.BuildFileTest;
import org.apache.tools.ant.BuildListener;
import org.apache.tools.ant.MagicNames;
import org.apache.tools.ant.MagicTestNames;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.ProjectHelper;
import org.apache.tools.ant.Target;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.UnknownElement;
import org.apache.tools.ant.util.ProcessUtil;
import org.dbunit.DatabaseUnitException;
import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.database.AbstractDatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.NoSuchTableException;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.dbunit.ext.mssql.InsertIdentityOperation;
import org.dbunit.ext.oracle.OracleDataTypeFactory;
import org.dbunit.operation.DatabaseOperation;
import org.dbunit.util.FileHelper;
import org.hsqldb.jdbc.JDBCDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 * Ant-based test class for the Dbunit ant task definition.
 *
 * @author Timothy Ruppert
 * @author Ben Cox
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since Jun 10, 2002
 * @see org.dbunit.ant.AntTest
 *
 *      Comment from {@link BuildFileTest}: 0deprecated as of 1.9.4. Use
 *      BuildFileRule, Assert, AntAssert and JUnit4 annotations to drive tests
 *      instead
 * @see org.apache.tools.ant.BuildFileRule
 */
public class DbUnitTaskIT {

    private static final String OUTPUT_DIR = "target/xml";

    private File outputDir;

    private Project project = new Project();

    private StringBuffer logBuffer = new StringBuffer();
    private StringBuffer fullLogBuffer = new StringBuffer();
    private StringBuffer outBuffer;
    private StringBuffer errBuffer;

    @BeforeClass
    public static void initDb() throws Exception {
        final Connection connection = JDBCDriver.getConnection("jdbc:hsqldb:mem:.", new Properties());

        final File ddlFile = new File("src/test/resources/sql/hypersonic.sql");
        final String sql = readSqlFromFile(ddlFile);

        try (final Statement statement = connection.createStatement();) {
            statement.execute(sql);
        }
    }

    private static String readSqlFromFile(final File ddlFile) throws IOException {
        final BufferedReader sqlReader = new BufferedReader(new FileReader(ddlFile));
        final StringBuilder sqlBuffer = new StringBuilder();
        while (sqlReader.ready()) {
            String line = sqlReader.readLine();
            if (!line.startsWith("-")) {
                sqlBuffer.append(line);
            }
        }

        sqlReader.close();

        return sqlBuffer.toString();
    }

    @Before
    public void setUp() throws Exception {
        String fileName = "src/test/resources/xml/antTestBuildFile.xml";
        File file = new File(fileName);
        assertTrue("Buildfile not found", file.isFile());

        System.clearProperty(MagicNames.PROJECT_BASEDIR);

        project.init();
        File antFile = new File(fileName);
        project.setUserProperty(MagicNames.ANT_FILE, antFile.getAbsolutePath());
        // set two new properties to allow to build unique names when running
        // multithreaded tests
        project.setProperty(MagicTestNames.TEST_PROCESS_ID, ProcessUtil.getProcessId("<Process>"));
        project.setProperty(MagicTestNames.TEST_THREAD_NAME, Thread.currentThread().getName());
        project.addBuildListener(new AntTestListener(Project.MSG_DEBUG));
        project.setUserProperty("dbunit.profile", "hsqldb");
        project.setUserProperty("dbunit.profile.driverClass", "org.hsqldb.jdbcDriver");
        project.setUserProperty("dbunit.profile.url", "jdbc:hsqldb:mem:.");
        project.setUserProperty("dbunit.profile.schema", "PUBLIC");
        project.setUserProperty("dbunit.profile.user", "sa");
        project.setUserProperty("dbunit.profile.password", "");
        project.setUserProperty("dbunit.profile.ddl", "hypersonic.sql");
        project.setUserProperty("dbunit.profile.unsupportedFeatures",
                "BLOB,CLOB,SCROLLABLE_RESULTSET,INSERT_IDENTITY,TRUNCATE_TABLE,SDO_GEOMETRY,XML_TYPE");
        project.setUserProperty("dbunit.profile.multiLineSupport", "true");
        ProjectHelper.configureProject(project, antFile);

        outputDir = new File(getProjectDir(), OUTPUT_DIR);
        outputDir.mkdirs();
    }

    @After
    public void tearDown() throws Exception {
        if (project != null) {
            if (project.getTargets().containsKey("tearDown")) {
                project.executeTarget("tearDown");
            }
        }

        outputDir = new File(getProjectDir(), OUTPUT_DIR);
        FileHelper.deleteDirectory(outputDir);
    }

    @Test
    public void tNoDriver() {
        expectBuildException("no-driver", "Should have required a driver attribute.");
    }

    @Test
    public void tNoDbUrl() {
        expectBuildException("no-db-url", "Should have required a url attribute.");
    }

    @Test
    public void testNoUserid() {
        expectBuildException("no-userid", "Should have required a userid attribute.");
    }

    @Test
    public void testNoPassword() {
        expectBuildException("no-password", "Should have required a password attribute.");
    }

    @Test
    public void testInvalidDatabaseInformation() {
        Throwable sql = null;
        try {
            executeTarget("invalid-db-info");
        } catch (BuildException e) {
            sql = e.getException();
        } finally {
            assertNotNull("Should have thrown a SQLException.", sql);
            assertTrue("Should have thrown a SQLException.", (sql instanceof SQLException));
        }
    }

    @Test
    public void testInvalidOperationType() {
        Throwable iae = null;
        try {
            executeTarget("invalid-type");
        } catch (BuildException e) {
            iae = e.getException();
        } finally {
            assertNotNull("Should have thrown an IllegalArgumentException.", iae);
            assertTrue("Should have thrown an IllegalArgumentException.", (iae instanceof IllegalArgumentException));
        }
    }

    @Test
    public void testSetFlatFalse() {
        String targetName = "set-format-xml";
        Operation operation = (Operation) getFirstStepFromTarget(targetName);
        assertTrue("Operation attribute format should have been 'xml', but was: " + operation.getFormat(),
                operation.getFormat().equalsIgnoreCase("xml"));
    }

    @Test
    public void testResolveOperationTypes() {
        assertOperationType("Should have been a NONE operation", "test-type-none", DatabaseOperation.NONE);
        assertOperationType("Should have been an DELETE_ALL operation", "test-type-delete-all",
                DatabaseOperation.DELETE_ALL);
        assertOperationType("Should have been an INSERT operation", "test-type-insert", DatabaseOperation.INSERT);
        assertOperationType("Should have been an UPDATE operation", "test-type-update", DatabaseOperation.UPDATE);
        assertOperationType("Should have been an REFRESH operation", "test-type-refresh", DatabaseOperation.REFRESH);
        assertOperationType("Should have been an CLEAN_INSERT operation", "test-type-clean-insert",
                DatabaseOperation.CLEAN_INSERT);
        assertOperationType("Should have been an CLEAN_INSERT operation", "test-type-clean-insert-composite",
                DatabaseOperation.CLEAN_INSERT);
        assertOperationType("Should have been an CLEAN_INSERT operation", "test-type-clean-insert-composite-combine",
                DatabaseOperation.CLEAN_INSERT);
        assertOperationType("Should have been an DELETE operation", "test-type-delete", DatabaseOperation.DELETE);
        assertOperationType("Should have been an MSSQL_INSERT operation", "test-type-mssql-insert",
                InsertIdentityOperation.INSERT);
        assertOperationType("Should have been an MSSQL_REFRESH operation", "test-type-mssql-refresh",
                InsertIdentityOperation.REFRESH);
        assertOperationType("Should have been an MSSQL_CLEAN_INSERT operation", "test-type-mssql-clean-insert",
                InsertIdentityOperation.CLEAN_INSERT);
    }

    @Test
    public void testInvalidCompositeOperationSrc() {
        expectBuildException("invalid-composite-operation-src",
                "Should have objected to nested operation src attribute " + "being set.");
    }

    @Test
    public void testInvalidCompositeOperationFlat() {
        expectBuildException("invalid-composite-operation-format-flat",
                "Should have objected to nested operation format attribute " + "being set.");
    }

    @Test
    public void testExportFull() {
        String targetName = "test-export-full";
        Export export = (Export) getFirstStepFromTarget(targetName);
        assertTrue("Should have been a flat format, " + "but was: " + export.getFormat(),
                export.getFormat().equalsIgnoreCase("flat"));
        List tables = export.getTables();
        assertTrue("Should have been an empty table list " + "(indicating a full dataset), but was: " + tables,
                tables.size() == 0);
    }

    @Test
    public void testExportPartial() {
        String targetName = "test-export-partial";
        Export export = (Export) getFirstStepFromTarget(targetName);
        List tables = export.getTables();
        assertEquals("table count", 2, tables.size());
        Table testTable = (Table) tables.get(0);
        Table pkTable = (Table) tables.get(1);
        assertTrue("Should have been been TABLE TEST_TABLE, but was: " + testTable.getName(),
                testTable.getName().equals("TEST_TABLE"));
        assertTrue("Should have been been TABLE PK_TABLE, but was: " + pkTable.getName(),
                pkTable.getName().equals("PK_TABLE"));
    }

    @Test
    public void testExportWithForwardOnlyResultSetTable() throws SQLException, DatabaseUnitException {
        String targetName = "test-export-forward-only-result-set-table-via-config";

        // Test if the correct result set table factory is set according to dbconfig
        Export export = (Export) getFirstStepFromTarget(targetName);
        DbUnitTask task = getFirstTargetTask(targetName);
        AbstractDatabaseConnection connection = task.createConnection();
        export.getExportDataSet(connection);
        assertEquals("org.dbunit.database.ForwardOnlyResultSetTableFactory",
                connection.getDatabaseConfig().getResultSetTableFactory().getClass().getName());
    }

    @Test
    public void testExportFlat() {
        String targetName = "test-export-format-flat";
        Export export = (Export) getFirstStepFromTarget(targetName);
        assertEquals("format", "flat", export.getFormat());
    }

    @Test
    public void testExportFlatWithDocytpe() {
        String targetName = "test-export-format-flat-with-doctype";
        Export export = (Export) getFirstStepFromTarget(targetName);
        assertEquals("format", "flat", export.getFormat());
        assertEquals("doctype", "dataset.dtd", export.getDoctype());
    }

    @Test
    public void testExportFlatWithEncoding() {
        String targetName = "test-export-format-flat-with-encoding";
        Export export = (Export) getFirstStepFromTarget(targetName);
        assertEquals("format", "flat", export.getFormat());
        assertEquals("encoding", "ISO-8859-1", export.getEncoding());
    }

    @Test
    public void testExportXml() {
        String targetName = "test-export-format-xml";
        Export export = (Export) getFirstStepFromTarget(targetName);
        assertTrue("Should have been an xml format, " + "but was: " + export.getFormat(),
                export.getFormat().equalsIgnoreCase("xml"));
    }

    @Test
    public void testExportCsv() {
        String targetName = "test-export-format-csv";
        Export export = (Export) getFirstStepFromTarget(targetName);
        assertTrue("Should have been a csv format, " + "but was: " + export.getFormat(),
                export.getFormat().equalsIgnoreCase("csv"));
    }

    @Test
    public void testExportDtd() {
        String targetName = "test-export-format-dtd";
        Export export = (Export) getFirstStepFromTarget(targetName);
        assertTrue("Should have been a dtd format, " + "but was: " + export.getFormat(),
                export.getFormat().equalsIgnoreCase("dtd"));
    }

    @Test
    public void testInvalidExportFormat() {
        expectBuildException("invalid-export-format", "Should have objected to invalid format attribute.");
    }

    @Test
    public void testExportXmlOrdered() throws Exception {
        String targetName = "test-export-format-xml-ordered";
        Export export = (Export) getFirstStepFromTarget(targetName);
        assertEquals("Should be ordered", true, export.isOrdered());
        assertTrue("Should have been an xml format, " + "but was: " + export.getFormat(),
                export.getFormat().equalsIgnoreCase("xml"));

        // Test if the correct dataset is created for ordered export
        DbUnitTask task = getFirstTargetTask(targetName);
        AbstractDatabaseConnection connection = task.createConnection();
        IDataSet dataSetToBeExported = export.getExportDataSet(connection);
        // Ordered export should use the filtered dataset
        assertEquals(dataSetToBeExported.getClass(), FilteredDataSet.class);
    }

    @Test
    public void testExportQuery() {
        String targetName = "test-export-query";
        Export export = (Export) getFirstStepFromTarget(targetName);
        assertEquals("format", "flat", export.getFormat());

        List queries = export.getTables();
        assertEquals("query count", 2, getQueryCount(queries));

        Query testTable = (Query) queries.get(0);
        assertEquals("name", "TEST_TABLE", testTable.getName());
        assertEquals("sql", "SELECT * FROM TEST_TABLE ORDER BY column0 DESC", testTable.getSql());

        Query pkTable = (Query) queries.get(1);
        assertEquals("name", "PK_TABLE", pkTable.getName());
        assertEquals("sql", "SELECT * FROM PK_TABLE", pkTable.getSql());
    }

    @Test
    public void testExportWithQuerySet() {
        String targetName = "test-export-with-queryset";
        Export export = (Export) getFirstStepFromTarget(targetName);
        assertEquals("format", "csv", export.getFormat());

        List queries = export.getTables();

        assertEquals("query count", 1, getQueryCount(queries));
        assertEquals("table count", 1, getTableCount(queries));
        assertEquals("queryset count", 2, getQuerySetCount(queries));

        Query secondTable = (Query) queries.get(0);
        assertEquals("name", "SECOND_TABLE", secondTable.getName());
        assertEquals("sql", "SELECT * FROM SECOND_TABLE", secondTable.getSql());

        QuerySet queryset1 = (QuerySet) queries.get(1);

        Query testTable = (Query) queryset1.getQueries().get(0);

        assertEquals("name", "TEST_TABLE", testTable.getName());

        QuerySet queryset2 = (QuerySet) queries.get(2);

        Query pkTable = (Query) queryset2.getQueries().get(0);
        Query testTable2 = (Query) queryset2.getQueries().get(1);

        assertEquals("name", "PK_TABLE", pkTable.getName());
        assertEquals("name", "TEST_TABLE", testTable2.getName());

        Table emptyTable = (Table) queries.get(3);

        assertEquals("name", "EMPTY_TABLE", emptyTable.getName());
    }

    @Test
    public void testWithBadQuerySet() {
        try {
            expectBuildException("invalid-queryset",
                    "Cannot specify 'id' and 'refid' attributes together in queryset.");
        } catch (AssertionError exc) {
            // ignoring for now. New version of ant swallows BuildException on id attribute
            // set.
        }
    }

    @Test
    public void testWithReferenceQuerySet() {
        String targetName = "test-queryset-reference";

        Export export = (Export) getFirstStepFromTarget(targetName);

        List tables = export.getTables();

        assertEquals("total count", 1, tables.size());

        QuerySet queryset = (QuerySet) tables.get(0);
        Query testTable = (Query) queryset.getQueries().get(0);
        Query secondTable = (Query) queryset.getQueries().get(1);

        assertEquals("name", "TEST_TABLE", testTable.getName());
        assertEquals("sql", "SELECT * FROM TEST_TABLE WHERE COLUMN0 = 'row0 col0'", testTable.getSql());

        assertEquals("name", "SECOND_TABLE", secondTable.getName());
        assertEquals("sql", "SELECT B.* FROM TEST_TABLE A, SECOND_TABLE B "
                + "WHERE A.COLUMN0 = 'row0 col0' AND B.COLUMN0 = A.COLUMN0", secondTable.getSql());

    }

    @Test
    public void testExportQueryMixed() {
        String targetName = "test-export-query-mixed";
        Export export = (Export) getFirstStepFromTarget(targetName);
        assertEquals("format", "flat", export.getFormat());

        List tables = export.getTables();
        assertEquals("total count", 2, tables.size());
        assertEquals("table count", 1, getTableCount(tables));
        assertEquals("query count", 1, getQueryCount(tables));

        Table testTable = (Table) tables.get(0);
        assertEquals("name", "TEST_TABLE", testTable.getName());

        Query pkTable = (Query) tables.get(1);
        assertEquals("name", "PK_TABLE", pkTable.getName());
    }

    /**
     * Tests the exception that is thrown when the compare fails because the source
     * format was different from the previous "export" task's write format.
     */
    @Test
    public void testExportAndCompareFormatMismatch() {
        String targetName = "test-export-and-compare-format-mismatch";

        try {
            getFirstTargetTask(targetName);
            fail("Should not be able to invoke ant task where the expected table was not found because it was tried to read in the wrong format.");
        } catch (BuildException expected) {
            Throwable cause = expected.getCause();
            assertTrue(cause instanceof DatabaseUnitException);
            DatabaseUnitException dbUnitException = (DatabaseUnitException) cause;
            String filename = new File(outputDir, "antExportDataSet.xml").toString();
            String expectedMsg = "Did not find table in source file '" + filename + "' using format 'xml'";
            assertEquals(expectedMsg, dbUnitException.getMessage());
            assertTrue(dbUnitException.getCause() instanceof NoSuchTableException);
            NoSuchTableException nstException = (NoSuchTableException) dbUnitException.getCause();
            assertEquals("TEST_TABLE", nstException.getMessage());
        }
    }

    @Test
    public void testDataTypeFactory() throws Exception {
        String targetName = "test-datatypefactory";
        DbUnitTask task = getFirstTargetTask(targetName);

        IDatabaseConnection connection = task.createConnection();
        IDataTypeFactory factory = connection.getDatabaseConfig().getDataTypeFactory();

        Class<OracleDataTypeFactory> expectedClass = OracleDataTypeFactory.class;
        assertEquals("factory", expectedClass, factory.getClass());
    }

    @Test
    public void testEscapePattern() throws Exception {
        String targetName = "test-escapepattern";
        DbUnitTask task = getFirstTargetTask(targetName);

        IDatabaseConnection connection = task.createConnection();
        String actualPattern = connection.getDatabaseConfig().getEscapePattern();

        String expectedPattern = "[?]";
        assertEquals("factory", expectedPattern, actualPattern);
    }

    @Test
    public void testDataTypeFactoryViaGenericConfig() throws Exception {
        String targetName = "test-datatypefactory-via-generic-config";
        DbUnitTask task = getFirstTargetTask(targetName);

        IDatabaseConnection connection = task.createConnection();

        DatabaseConfig config = connection.getDatabaseConfig();

        IDataTypeFactory factory = config.getDataTypeFactory();
        Class<OracleDataTypeFactory> expectedClass = OracleDataTypeFactory.class;
        assertEquals("factory", expectedClass, factory.getClass());

        String[] actualTableType = config.getTableTypes();
        assertArrayEquals("tableType", new String[] { "TABLE", "SYNONYM" }, actualTableType);
        assertTrue("batched statements feature should be true", connection.getDatabaseConfig().isBatchedStatements());
        assertTrue("qualified tablenames feature should be true",
                connection.getDatabaseConfig().isCaseSensitiveTableNames());
    }

    @Test
    public void testClasspath() throws Exception {
        String targetName = "test-classpath";

        try {
            executeTarget(targetName);
            fail("Should not be able to connect with invalid url!");
        } catch (BuildException e) {
            // Verify exception type
            assertTrue("nested exxception type", e.getException() instanceof SQLException);
        }

    }

    @Test
    public void testDriverNotInClasspath() throws Exception {
        String targetName = "test-drivernotinclasspath";

        try {
            executeTarget(targetName);
            fail("Should not have found driver!");
        } catch (BuildException e) {
            // Verify exception type
            assertEquals("nested exception type", ClassNotFoundException.class, e.getException().getClass());
        }
    }

    @Test
    public void testReplaceOperation() throws Exception {
        String targetName = "test-replace";
        final IDatabaseTester dbTest = new JdbcDatabaseTester("org.hsqldb.jdbcDriver", "jdbc:hsqldb:mem:.", "sa", "",
                "PUBLIC");
        executeTarget(targetName);
        final IDataSet ds = dbTest.getConnection().createDataSet();
        final ITable table = ds.getTable("PK_TABLE");
        assertNull(table.getValue(0, "NORMAL0"));
        assertEquals("row 1", table.getValue(1, "NORMAL0"));
    }

    @Test
    public void testOrderedOperation() throws Exception {
        String targetName = "test-ordered";
        final IDatabaseTester dbTest = new JdbcDatabaseTester("org.hsqldb.jdbcDriver", "jdbc:hsqldb:mem:.", "sa", "",
                "PUBLIC");
        executeTarget(targetName);
        final IDataSet ds = dbTest.getConnection().createDataSet();
        final ITable table = ds.getTable("PK_TABLE");
        assertEquals("row 0", table.getValue(0, "NORMAL0"));
        assertEquals("row 1", table.getValue(1, "NORMAL0"));
    }

    @Test
    public void testReplaceOrderedOperation() throws Exception {
        String targetName = "test-replace-ordered";
        final IDatabaseTester dbTest = new JdbcDatabaseTester("org.hsqldb.jdbcDriver", "jdbc:hsqldb:mem:.", "sa", "",
                "PUBLIC");
        executeTarget(targetName);
        final IDataSet ds = dbTest.getConnection().createDataSet();
        final ITable table = ds.getTable("PK_TABLE");
        assertNull(table.getValue(0, "NORMAL0"));
        assertEquals("row 1", table.getValue(1, "NORMAL0"));
    }

    private void assertOperationType(String failMessage, String targetName, DatabaseOperation expected) {
        Operation oper = (Operation) getFirstStepFromTarget(targetName);
        DatabaseOperation dbOper = oper.getDbOperation();
        assertTrue(failMessage + ", but was: " + dbOper, expected.equals(dbOper));
    }

    private int getQueryCount(List tables) {
        int count = 0;
        for (Object table : tables) {
            if (table instanceof Query) {
                count++;
            }
        }

        return count;
    }

    private int getTableCount(List tables) {
        int count = 0;
        for (Object table : tables) {
            if (table instanceof Table) {
                count++;
            }
        }

        return count;
    }

    private int getQuerySetCount(List tables) {
        int count = 0;
        for (Object table : tables) {
            if (table instanceof QuerySet) {
                count++;
            }
        }

        return count;
    }

    private DbUnitTaskStep getFirstStepFromTarget(String targetName) {
        return getStepFromTarget(targetName, 0);
    }

    private DbUnitTaskStep getStepFromTarget(String targetName, int index) {
        DbUnitTask task = getFirstTargetTask(targetName);
        List steps = task.getSteps();
        if (steps == null || steps.size() == 0) {
            fail("Can't get a dbunit <step> from the target: " + targetName + ". No steps available.");
        }
        return (DbUnitTaskStep) steps.get(index);
    }

    private DbUnitTask getFirstTargetTask(String targetName) {
        Hashtable<String, Target> targets = project.getTargets();
        executeTarget(targetName);
        Target target = targets.get(targetName);

        Task[] tasks = target.getTasks();
        for (Task task2 : tasks) {
            Object task = task2;
            if (task instanceof UnknownElement) {
                ((UnknownElement) task).maybeConfigure(); // alternative to this is setting id on dbunit task. then ant
                                                          // will not clean realThing
                task = ((UnknownElement) task).getRealThing();
            }
            if (task instanceof DbUnitTask) {
                return (DbUnitTask) task;
            }
        }

        return null;
    }

    /**
     * run a target, expect for any build exception
     *
     * @param target target to run
     * @param cause  information string to reader of report
     */
    private void expectBuildException(String target, String cause) {
        expectSpecificBuildException(target, cause, null);
    }

    /**
     * Executes a target we have set up
     *
     * @pre configureProject has been called
     * @param targetName target to run
     */
    private void executeTarget(String targetName) {
        PrintStream sysOut = System.out;
        PrintStream sysErr = System.err;
        try {
            sysOut.flush();
            sysErr.flush();
            outBuffer = new StringBuffer();
            PrintStream out = new PrintStream(new AntOutputStream(outBuffer));
            System.setOut(out);
            errBuffer = new StringBuffer();
            PrintStream err = new PrintStream(new AntOutputStream(errBuffer));
            System.setErr(err);
            logBuffer = new StringBuffer();
            fullLogBuffer = new StringBuffer();
            project.executeTarget(targetName);
        } finally {
            System.setOut(sysOut);
            System.setErr(sysErr);
        }

    }

    /**
     * Gets the directory of the project.
     *
     * @return the base dir of the project
     */
    private File getProjectDir() {
        return project.getBaseDir();
    }

    /**
     * Runs a target, wait for a build exception.
     *
     * @param target target to run
     * @param cause  information string to reader of report
     * @param msg    the message value of the build exception we are waiting for set
     *               to null for any build exception to be valid
     */
    private void expectSpecificBuildException(String target, String cause, String msg) {
        try {
            executeTarget(target);
        } catch (BuildException ex) {
            assertTrue("Should throw BuildException because '" + cause + "' with message '" + msg
                    + "' (actual message '" + ex.getMessage() + "' instead)",
                    msg == null || ex.getMessage().equals(msg));
            return;
        }
        fail("Should throw BuildException because: " + cause);
    }

    /**
     * an output stream which saves stuff to our buffer.
     */
    private static class AntOutputStream extends OutputStream {

        private StringBuffer buffer;

        public AntOutputStream(StringBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public void write(int b) {
            buffer.append((char) b);
        }
    }

    /**
     * Our own personal build listener.
     */
    private class AntTestListener implements BuildListener {

        private int logLevel;

        /**
         * Constructs a test listener which will ignore log events above the given
         * level.
         */
        public AntTestListener(int logLevel) {
            this.logLevel = logLevel;
        }

        /**
         * Fired before any targets are started.
         */
        @Override
        public void buildStarted(BuildEvent event) {
        }

        /**
         * Fired after the last target has finished. This event will still be thrown if
         * an error occurred during the build.
         *
         * @see BuildEvent#getException()
         */
        @Override
        public void buildFinished(BuildEvent event) {
        }

        /**
         * Fired when a target is started.
         *
         * @see BuildEvent#getTarget()
         */
        @Override
        public void targetStarted(BuildEvent event) {
            // System.out.println("targetStarted " + event.getTarget().getName());
        }

        /**
         * Fired when a target has finished. This event will still be thrown if an error
         * occurred during the build.
         *
         * @see BuildEvent#getException()
         */
        @Override
        public void targetFinished(BuildEvent event) {
            // System.out.println("targetFinished " + event.getTarget().getName());
        }

        /**
         * Fired when a task is started.
         *
         * @see BuildEvent#getTask()
         */
        @Override
        public void taskStarted(BuildEvent event) {
            // System.out.println("taskStarted " + event.getTask().getTaskName());
        }

        /**
         * Fired when a task has finished. This event will still be throw if an error
         * occurred during the build.
         *
         * @see BuildEvent#getException()
         */
        @Override
        public void taskFinished(BuildEvent event) {
            // System.out.println("taskFinished " + event.getTask().getTaskName());
        }

        /**
         * Fired whenever a message is logged.
         *
         * @see BuildEvent#getMessage()
         * @see BuildEvent#getPriority()
         */
        @Override
        public void messageLogged(BuildEvent event) {
            if (event.getPriority() > logLevel) {
                // ignore event
                return;
            }

            if (event.getPriority() == Project.MSG_INFO || event.getPriority() == Project.MSG_WARN
                    || event.getPriority() == Project.MSG_ERR) {
                logBuffer.append(event.getMessage());
            }
            fullLogBuffer.append(event.getMessage());
        }
    }

}
