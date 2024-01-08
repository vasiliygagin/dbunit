/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 *
 */
public class GenericEnvironment extends DatabaseEnvironment {

    /**
     * @param profile
     * @param databaseConfig
     * @throws Exception
     */
    protected GenericEnvironment(String profileName) throws Exception {
        super(new GenericDatabaseProfile(profileName), new DatabaseConfig());
        // TODO Auto-generated constructor stub
    }

    private static class GenericDatabaseProfile extends DatabaseProfile {

        /**
         * @param profileName
         */
        private GenericDatabaseProfile(String profileName) throws IOException {
            this(getProperties(profileName));
        }

        private GenericDatabaseProfile(Properties properties) {
            super(properties.getProperty("dbunit.profile.driverClass"), properties.getProperty("dbunit.profile.url"),
                    properties.getProperty("dbunit.profile.schema", null),
                    properties.getProperty("dbunit.profile.user"), properties.getProperty("dbunit.profile.password"),
                    properties.getProperty("dbunit.profile.ddl"),
                    Boolean.valueOf(properties.getProperty("dbunit.profile.multiLineSupport")),
                    parseStringArray(properties.getProperty("dbunit.profile.unsupportedFeatures")));
        }

        private static final Logger logger = LoggerFactory.getLogger(GenericDatabaseProfile.class);

        private static String[] parseStringArray(String property) {
            // If property is not set return an empty array
            if (property == null) {
                return new String[] {};
            }

            List<String> stringList = new ArrayList<>();
            StringTokenizer tokenizer = new StringTokenizer(property, ",");
            while (tokenizer.hasMoreTokens()) {
                stringList.add(tokenizer.nextToken().trim());
            }
            return stringList.toArray(new String[stringList.size()]);
        }

        private static Properties getProperties(String profileName) throws IOException {

            final Properties properties = System.getProperties();
            if (profileName == null) {
                profileName = properties.getProperty("dbunit.profile");
            }
            if (profileName == null) {
                profileName = "hsqldb";
            }
            logger.info("Selected profile '{}'", profileName);

            String fileName = profileName + "-dbunit.properties";

            final InputStream inputStream = DatabaseEnvironment.class.getClassLoader().getResourceAsStream(fileName);
            if (inputStream != null) {
                properties.load(inputStream);
                logger.info("Loaded properties from file '{}'", fileName);
                inputStream.close();
            } else {
                logger.warn("Properties file '{}' is not found", fileName);
            }
            return properties;
        }
    }

}
