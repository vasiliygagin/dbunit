/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.dbunit.database.AbstractDatabaseConnection;
import org.dbunit.junit.DatabaseException;

public final class SqlScriptExecutor {

    public static void execute(AbstractDatabaseConnection connection, String filePath) throws DatabaseException {
        File file = new File(filePath).getAbsoluteFile();
        SqlScriptExecutor.execute(connection, file);
    }

    public static void execute(AbstractDatabaseConnection connection, File ddlFile) throws DatabaseException {
        String sql;
        try {
            sql = readSqlFromFile(ddlFile);
        } catch (IOException exc) {
            throw new DatabaseException("Unable to read from source file [" + ddlFile + "]", exc);
        }

        List<String> sqlStatements = splitIntoStatements(sql);

        for (String sqlStatement : sqlStatements) {
            try (Statement statement = connection.getConnection().createStatement();) {
                statement.execute(sqlStatement);
            } catch (SQLException exc) {
                throw new DatabaseException("Unable to execute script [" + ddlFile + "]", exc);
            }
        }

    }

    static String readSqlFromFile(final File ddlFile) throws IOException {
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

    static List<String> splitIntoStatements(String sql) {
        List<String> sqlStatements = new ArrayList<>();
        StringTokenizer tokenizer = new StringTokenizer(sql, ";");
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            token = token.trim();
            if (token.length() > 0) {
                sqlStatements.add(token);
            }
        }
        return sqlStatements;
    }
}
