/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit4;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public final class DdlExecutor {

    public static void executeDdl(Connection connection, final File ddlFile) throws IOException, SQLException {
        String sql = readSqlFromFile(ddlFile);

        try (Statement statement = connection.createStatement();) {
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
}
