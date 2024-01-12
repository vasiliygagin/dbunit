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

package org.dbunit.ext.db2;

import java.sql.Connection;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.metadata.MetadataManager;

/**
 * Database connection for DB2 that pre-configures all properties required to
 * successfully use dbunit with DB2.
 *
 * @author Federico Spinazzi
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since 1.5.5 (Jul 17, 2003)
 * @deprecated VG: No need for separate class for this. Just need separate config.
 */
@Deprecated
public class Db2Connection extends DatabaseConnection {

    public Db2Connection(Connection connection, String schema, MetadataManager metadataManager)
            throws DatabaseUnitException {
        super(connection, buildConfig(), schema, metadataManager);
    }

    static DatabaseConfig buildConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataTypeFactory(new Db2DataTypeFactory());
        config.setMetadataHandler(new Db2MetadataHandler());
        return config;
    }
}
