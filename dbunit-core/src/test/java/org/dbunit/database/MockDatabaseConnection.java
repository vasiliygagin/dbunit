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

package org.dbunit.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.dbunit.database.statement.IStatementFactory;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.filter.SequenceTableFilter;

import com.mockobjects.ExpectationCounter;
import com.mockobjects.Verifiable;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Mar 16, 2002
 */
public class MockDatabaseConnection implements IDatabaseConnection, Verifiable {

    private ExpectationCounter _closeCalls = new ExpectationCounter("MockDatabaseConnection.close");

    private Connection _connection;
    private String _schema;
    private IDataSet _dataSet;
//    private IStatementFactory _statementFactory;
    private DatabaseConfig _databaseConfig = new DatabaseConfig();

    public void setupSchema(String schema) {
        _schema = schema;
    }

    public void setupConnection(Connection connection) {
        _connection = connection;
    }

    public void setupDataSet(IDataSet dataSet) {
        _dataSet = dataSet;
    }

    public void setupDataSet(ITable table) throws AmbiguousTableNameException {
        _dataSet = new DefaultDataSet(table);
    }

    public void setupDataSet(ITable[] tables) throws AmbiguousTableNameException {
        _dataSet = new DefaultDataSet(tables);
    }

    public void setupStatementFactory(IStatementFactory statementFactory) {
        _databaseConfig.setStatementFactory(statementFactory);
    }

//    public void setupEscapePattern(String escapePattern)
//    {
//        _databaseConfig.setProperty(DatabaseConfig.PROPERTY_ESCAPE_PATTERN, escapePattern);
//    }
//
    public void setExpectedCloseCalls(int callsCount) {
        _closeCalls.setExpected(callsCount);
    }

    ///////////////////////////////////////////////////////////////////////////
    // Verifiable interface

    @Override
    public void verify() {
        _closeCalls.verify();
    }

    ///////////////////////////////////////////////////////////////////////////
    // IDatabaseConnection interface

    @Override
    public Connection getConnection() throws SQLException {
        return _connection;
    }

    @Override
    public String getSchema() {
        return _schema;
    }

    @Override
    public void close() throws SQLException {
        _closeCalls.inc();
    }

    @Override
    public IDataSet createDataSet() throws SQLException {
        return _dataSet;
    }

    @Override
    public IDataSet createDataSet(String[] tableNames) throws SQLException, AmbiguousTableNameException {
        IDataSet dataSet = createDataSet();
        SequenceTableFilter filter = new SequenceTableFilter(tableNames, dataSet.isCaseSensitiveTableNames());
        return new FilteredDataSet(filter, dataSet);
    }

    @Override
    public IResultSetTable createQueryTable(String resultName, String sql) throws DataSetException, SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ITable createTable(String tableName, PreparedStatement preparedStatement)
            throws DataSetException, SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ITable createTable(String tableName) throws DataSetException, SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRowCount(String tableName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRowCount(String tableName, String whereClause) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IStatementFactory getStatementFactory() {
        return _databaseConfig.getStatementFactory();
    }

    @Override
    public DatabaseConfig getDatabaseConfig() {
        return _databaseConfig;
    }
}
