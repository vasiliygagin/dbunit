/*
 *
 * The DbUnit Database Testing Framework
 * Copyright (C)2002-2010, DbUnit.org
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

package org.dbunit.testutil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * @author John Hurst (john.b.hurst@gmail.com)
 * @since 2.4.8
 */
public class TestUtils {

    public static String getFileName(String fileName) {
        return "src/test/resources/" + fileName;
    }

    public static File getFile(String fileName) {
        return new File(getFileName(fileName)).getAbsoluteFile();
    }

    public static FileReader getFileReader(String fileName) throws FileNotFoundException {
        return new FileReader(getFile(fileName));
    }

    public static FileInputStream getFileInputStream(String fileName) throws FileNotFoundException {
        return new FileInputStream(getFileName(fileName));
    }

}
