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

import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.statement.IStatementFactory;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.dbunit.dataset.filter.IColumnFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration used by the {@link DatabaseConnection}.
 *
 * @author manuel.laflamme
 * @author gommma (gommma AT users.sourceforge.net)
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since 2.0
 * @deprecated
 */
@Deprecated
public class DatabaseConfig extends io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig {

    /**
     * Logger for this class
     */
    private static final Logger logger = LoggerFactory.getLogger(DatabaseConfig.class);

    private static final String PREFIX = "http://www.dbunit.org/";

    public static final String PROPERTY_STATEMENT_FACTORY = PREFIX + "properties/statementFactory";
    public static final String PROPERTY_RESULTSET_TABLE_FACTORY = PREFIX + "properties/resultSetTableFactory";
    public static final String PROPERTY_DATATYPE_FACTORY = PREFIX +"properties/datatypeFactory";
    public static final String PROPERTY_ESCAPE_PATTERN = PREFIX +"properties/escapePattern";
    public static final String PROPERTY_TABLE_TYPE = PREFIX +"properties/tableType";
    public static final String PROPERTY_PRIMARY_KEY_FILTER = PREFIX +"properties/primaryKeyFilter";
    public static final String PROPERTY_BATCH_SIZE = PREFIX +"properties/batchSize";
    public static final String PROPERTY_FETCH_SIZE = PREFIX +"properties/fetchSize";
    public static final String PROPERTY_METADATA_HANDLER = PREFIX +"properties/metadataHandler";
    public static final String PROPERTY_ALLOW_VERIFYTABLEDEFINITION_EXPECTEDTABLE_COUNT_MISMATCH = PREFIX +"properties/allowVerifytabledefinitionExpectedtableCountMismatch";
    public static final String PROPERTY_IDENTITY_COLUMN_FILTER = PREFIX +"properties/mssql/identityColumnFilter";

    public static final String FEATURE_CASE_SENSITIVE_TABLE_NAMES = PREFIX +"features/caseSensitiveTableNames";
    public static final String FEATURE_QUALIFIED_TABLE_NAMES = PREFIX +"features/qualifiedTableNames";
    public static final String FEATURE_BATCHED_STATEMENTS = PREFIX +"features/batchedStatements";
    public static final String FEATURE_DATATYPE_WARNING = PREFIX +"features/datatypeWarning";
    public static final String FEATURE_SKIP_ORACLE_RECYCLEBIN_TABLES = PREFIX +"features/skipOracleRecycleBinTables";
    public static final String FEATURE_ALLOW_EMPTY_FIELDS = PREFIX +"features/allowEmptyFields";

    /**
     * A list of all properties as {@link ConfigProperty} objects. The objects
     * contain the allowed java type and whether or not a property is nullable.
     */
    public static final ConfigProperty<?>[] ALL_PROPERTIES = {
            new ConfigProperty<>(PROPERTY_STATEMENT_FACTORY, IStatementFactory.class, false,
                    DatabaseConfig::getStatementFactory, DatabaseConfig::setStatementFactory),
            new ConfigProperty<>(PROPERTY_RESULTSET_TABLE_FACTORY, IResultSetTableFactory.class, false,
                    DatabaseConfig::getResultSetTableFactory, DatabaseConfig::setResultSetTableFactory),
            new ConfigProperty<>(PROPERTY_DATATYPE_FACTORY, IDataTypeFactory.class, false,
                    DatabaseConfig::getDataTypeFactory, DatabaseConfig::setDataTypeFactory),
            new ConfigProperty<>(PROPERTY_ESCAPE_PATTERN, String.class, true, DatabaseConfig::getEscapePattern,
                    DatabaseConfig::setEscapePattern),
            new ConfigProperty<>(PROPERTY_TABLE_TYPE, String[].class, false, DatabaseConfig::getTableTypes,
                    DatabaseConfig::setTableTypes),
            new ConfigProperty<>(PROPERTY_PRIMARY_KEY_FILTER, IColumnFilter.class, true,
                    DatabaseConfig::getPrimaryKeysFilter, DatabaseConfig::setPrimaryKeysFilter),
            new ConfigProperty<>(PROPERTY_BATCH_SIZE, Integer.class, false, DatabaseConfig::getBatchSize,
                    DatabaseConfig::setBatchSize),
            new ConfigProperty<>(PROPERTY_FETCH_SIZE, Integer.class, false, DatabaseConfig::getFetchSize,
                    DatabaseConfig::setFetchSize),
            new ConfigProperty<>(PROPERTY_METADATA_HANDLER, IMetadataHandler.class, false,
                    DatabaseConfig::getMetadataHandler, DatabaseConfig::setMetadataHandler),
            new ConfigProperty<>(PROPERTY_IDENTITY_COLUMN_FILTER, IColumnFilter.class, true,
                    DatabaseConfig::getIdentityFilter, DatabaseConfig::setIdentityFilter),
            new ConfigProperty<>(FEATURE_CASE_SENSITIVE_TABLE_NAMES, Boolean.class, false,
                    DatabaseConfig::isCaseSensitiveTableNames, DatabaseConfig::setCaseSensitiveTableNames),
            new ConfigProperty<>(FEATURE_QUALIFIED_TABLE_NAMES, Boolean.class, false,
                    DatabaseConfig::isQualifiedTableNames, DatabaseConfig::setQualifiedTableNames),
            new ConfigProperty<>(FEATURE_BATCHED_STATEMENTS, Boolean.class, false, DatabaseConfig::isBatchedStatements,
                    DatabaseConfig::setBatchedStatements),
            new ConfigProperty<>(FEATURE_DATATYPE_WARNING, Boolean.class, false, DatabaseConfig::isDatatypeWarning,
                    DatabaseConfig::setDatatypeWarning),
            new ConfigProperty<>(FEATURE_SKIP_ORACLE_RECYCLEBIN_TABLES, Boolean.class, false,
                    DatabaseConfig::isSkipOracleRecycleBinTables, DatabaseConfig::setSkipOracleRecycleBinTables),
            new ConfigProperty<>(FEATURE_ALLOW_EMPTY_FIELDS, Boolean.class, false, DatabaseConfig::isAllowEmptyFields,
                    DatabaseConfig::setAllowEmptyFields),
            new ConfigProperty<>(PROPERTY_ALLOW_VERIFYTABLEDEFINITION_EXPECTEDTABLE_COUNT_MISMATCH, Boolean.class,
                    false, DatabaseConfig::isAllowCountMismatch, DatabaseConfig::setAllowCountMismatch), //
    };

    /**
     * A list of all features as strings
     *
     * @deprecated since 2.4.7 Use the {@link #ALL_PROPERTIES} where features are
     *             listed now as well
     */
    @Deprecated
    public static final String[] ALL_FEATURES = { FEATURE_CASE_SENSITIVE_TABLE_NAMES, FEATURE_QUALIFIED_TABLE_NAMES,
            FEATURE_BATCHED_STATEMENTS, FEATURE_DATATYPE_WARNING, FEATURE_SKIP_ORACLE_RECYCLEBIN_TABLES,
            FEATURE_ALLOW_EMPTY_FIELDS };

    public DatabaseConfig() {
    }

    public DatabaseConfig(io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig databaseConfig) {
        apply(databaseConfig);
    }

    /**
     * Set the value of a feature flag.
     *
     * @param name  the feature id
     * @param value the feature status
     * @deprecated since 2.4.7 Use the {@link #setProperty(String, Object)} also for
     *             features
     */
    @Deprecated
    public void setFeature(String name, boolean value) {
        logger.trace("setFeature(name={}, value={}) - start", name, value);

        setProperty(name, value);
    }

    /**
     * Look up the value of a feature flag.
     *
     * @param name the feature id
     * @return the feature status
     * @deprecated since 2.4.7 Use the {@link #getProperty(String)} where features
     *             are listed now as well
     */
    @Deprecated
    public boolean getFeature(String name) {
        Object property = getProperty(name);
        boolean result;
        if (property == null) {
            result = false;
        } else if (property instanceof Boolean) {
            result = (Boolean) property;
        } else {
            String propString = String.valueOf(property);
            result = Boolean.parseBoolean(propString);
        }
        return result;
    }

    /**
     * Set the value of a property.
     *
     * @param fullName  the property id
     * @param value the property value
     */
    public void setProperty(String fullName, Object value) {

        ConfigProperty<?> prop = findByName(fullName);
        if (prop == null) {
            throw new IllegalArgumentException("Did not find property with name '" + fullName + "'");
        }
        setProperty(prop, value);
    }

    private void setProperty(ConfigProperty prop, Object value) {
        value = convertIfNeeded(prop, value);

        // Validate if the type of the given object is correct
        checkObjectAllowed(prop, value);

        // If we get here the type is allowed (no exception was thrown)
        prop.setter.accept(this, value);
    }

    /**
     * Look up the value of a property.
     *
     * @param fullName the property id
     * @return the property value
     */
    public <T> T getProperty(String fullName) {
        ConfigProperty prop = findByName(fullName);
        if (prop == null) {
            return null;
        }

        return (T) prop.getter.apply(this);
    }

    private Object convertIfNeeded(ConfigProperty prop, Object value) {
        Class allowedPropType = prop.getPropertyType();
        if (allowedPropType == Boolean.class || allowedPropType == boolean.class) {
            // String -> Boolean is a special mapping which is allowed
            if (value instanceof String) {
                return Boolean.valueOf((String) value);
            }
        }

        return value;
    }

    /**
     * Checks whether the given value has the correct java type for the given
     * property. If the value is not allowed for the given property an
     * {@link IllegalArgumentException} is thrown.
     *
     * @param propertyName The property to be set
     * @param value    The value to which the property should be set
     */
    protected void checkObjectAllowed(String propertyName, Object value) {

        ConfigProperty prop = findByName(propertyName);
        if (prop != null) {
            // First check for null
            checkObjectAllowed(prop, value);
        } else {
            logger.info("Unknown property '" + prop.property + "'. Cannot validate the type of the object to be set."
                    + " Please notify a developer to update the list of properties.");
        }
    }

    private void checkObjectAllowed(ConfigProperty prop, Object value) {
        if (value == null) {
            if (!prop.isNullable()) {
                throw new IllegalArgumentException("The property '" + prop.property + "' is not nullable.");
            }
        } else {
            Class allowedPropType = prop.getPropertyType();
            if (!allowedPropType.isAssignableFrom(value.getClass())) {
                throw new IllegalArgumentException("Cannot cast object of type '" + value.getClass()
                        + "' to allowed type '" + allowedPropType + "'.");
            }
        }
    }

    /**
     * Sets the given properties on the {@link DatabaseConfig} instance using the
     * given String values. This is useful to set properties configured as strings
     * by a build tool like ant or maven. If the required property type is an object
     * it uses reflection to create an instance of the class specified as string.
     *
     * @param stringProperties The properties as strings. The key of the properties
     *                         can be either the long or the short name.
     * @throws DatabaseUnitException
     */
    public void setPropertiesByString(Properties stringProperties) throws DatabaseUnitException {
        for (Entry<Object, Object> entry : stringProperties.entrySet()) {
            String propKey = (String) entry.getKey();
            String propValue = (String) entry.getValue();

            setPropertyByString(propKey, propValue);
        }
    }

    private void setPropertyByString(String propertyName, String propertyValue) throws DatabaseUnitException {
        ConfigProperty<?> property = findByFullOrShortName(propertyName);

        if (property == null) {
            logger.info("Could not set property '{}' - not found in the list of known properties.", propertyName);
        } else {
            Object obj = createObjectFromString(property, propertyValue);

            setProperty(property, obj);
        }
    }

    private ConfigProperty<?> findByFullOrShortName(String propKey) {
        ConfigProperty<?> dbunitProp = findByName(propKey);
        if (dbunitProp == null) {
            dbunitProp = findByShortName(propKey);
        }
        return dbunitProp;
    }

    private <T> T createObjectFromString(ConfigProperty<T> dbunitProp, String propValue) throws DatabaseUnitException {
        if (dbunitProp == null) {
            throw new NullPointerException("The parameter 'dbunitProp' must not be null");
        }
        if (propValue == null) {
            // Null must not be casted
            return null;
        }

        Class targetClass = dbunitProp.getPropertyType();
        if (targetClass == String.class) {
            return (T) propValue;
        } else if (targetClass == Boolean.class) {
            return (T) Boolean.valueOf(propValue);
        } else if (targetClass == String[].class) {
            String[] result = propValue.split(",");
            for (int i = 0; i < result.length; i++) {
                result[i] = result[i].trim();
            }
            return (T) result;
        } else if (targetClass == Integer.class) {
            return (T) new Integer(propValue);
        } else {
            // Try via reflection
            return (T) createInstance(propValue);
        }
    }

    private Object createInstance(String className) throws DatabaseUnitException {
        // Setup data type factory for example.
        try {
            Object o = Class.forName(className).newInstance();
            return o;
        } catch (ClassNotFoundException e) {
            throw new DatabaseUnitException("Class Not Found: '" + className + "' could not be loaded", e);
        } catch (IllegalAccessException e) {
            throw new DatabaseUnitException("Illegal Access: '" + className + "' could not be loaded", e);
        } catch (InstantiationException e) {
            throw new DatabaseUnitException("Instantiation Exception: '" + className + "' could not be loaded", e);
        }
    }

    /**
     * Searches the {@link ConfigProperty} object for the property with the given
     * name
     *
     * @param propertyName The property for which the enumerated object should be
     *                 resolved
     * @return The property object or <code>null</code> if it was not found.
     */
    public static final ConfigProperty<?> findByName(String propertyName) {
        for (ConfigProperty<?> property : ALL_PROPERTIES) {
            if (property.getProperty().equals(propertyName)) {
                return property;
            }
        }

        return null;
    }

    /**
     * Searches the {@link ConfigProperty} object for the property with the given
     * name
     *
     * @param shortPropertyName The property short name for which the enumerated object
     *                      should be resolved. Example: the short name of
     *                      {@value #PROPERTY_FETCH_SIZE} is <code>fetchSize</code>
     *                      which is the last part of the fully qualified URL.
     * @return The property object or <code>null</code> if it was not found.
     */
    public static final ConfigProperty<?> findByShortName(String shortPropertyName) {
        for (ConfigProperty<?> property : DatabaseConfig.ALL_PROPERTIES) {
            String fullProperty = property.getProperty();
            if (fullProperty.endsWith(shortPropertyName)) {
                return property;
            }
        }

        return null;
    }

    /**
     * @author gommma (gommma AT users.sourceforge.net)
     * @author Last changed by: $Author$
     * @version $Revision$ $Date$
     * @since 2.4.0
     */
    public static class ConfigProperty<T> {
        private String property;
        private Class<T> propertyType;
        private boolean nullable;
        private final Function<? extends DatabaseConfig, T> getter;
        private final BiConsumer<? extends DatabaseConfig, T> setter;

        public ConfigProperty(String property, Class<T> propertyType, boolean nullable,
                Function<? extends DatabaseConfig, T> getter, BiConsumer<? extends DatabaseConfig, T> setter) {
            if (property == null) {
                throw new NullPointerException("The parameter 'property' must not be null");
            }
            if (propertyType == null) {
                throw new NullPointerException("The parameter 'propertyType' must not be null");
            }

            this.property = property;
            this.propertyType = propertyType;
            this.nullable = nullable;
            this.getter = getter;
            this.setter = setter;
        }

        public String getProperty() {
            return property;
        }

        public Class<?> getPropertyType() {
            return propertyType;
        }

        public boolean isNullable() {
            return nullable;
        }

        @Override
        public int hashCode() {
            return Objects.hash(property);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ConfigProperty other = (ConfigProperty) obj;
            if (!Objects.equals(property, other.property)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return getClass().getName() + "[" +
                    "property=" + property +
                    ", propertyType=" + propertyType +
                    ", nullable=" + nullable +
                    "]";
        }
    }
}
