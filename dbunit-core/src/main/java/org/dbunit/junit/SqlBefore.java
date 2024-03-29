/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation registers given {@link javax.sql.DataSource} implementations with dbUnit.
 * Name should be unique per test class.
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD })
@Repeatable(SqlBefores.class)
public @interface SqlBefore {

    /**
     * @return Name, to register dataSource under.
     */
    String filePath();

    /**
     * @return Name of the registered dataSource. Not required if there is only one dataSource configured for test.
     */
    String dataSourceName() default "";
}
