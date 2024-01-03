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

package org.dbunit.dataset.xml;

import java.io.InputStream;
import java.io.Reader;

import org.dbunit.dataset.CachedDataSet;
import org.dbunit.dataset.DataSetException;
import org.xml.sax.InputSource;

/**
 * Reads and writes original XML dataset document. This format is very verbose
 * and must conform to the following DTD:
 *
 * <pre>
&lt;?xml version="1.0" encoding="UTF-8"?&gt;
&lt;!ELEMENT dataset (table+)&gt;
&lt;!ELEMENT table (column*, row*)&gt;
&lt;!ATTLIST table name CDATA #REQUIRED&gt;
&lt;!ELEMENT column (#PCDATA)&gt;
&lt;!ELEMENT row (value | null | none)*&gt;
&lt;!ELEMENT value (#PCDATA)&gt;
&lt;!ELEMENT null EMPTY&gt;
&lt;!ELEMENT none EMPTY&gt;
 * </pre>
 *
 *
 * @author Manuel Laflamme
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since 1.0 (Feb 17, 2002)
 */
public class XmlDataSet extends CachedDataSet {

    /**
     * Creates an XmlDataSet with the specified xml reader.
     */
    public XmlDataSet(Reader reader) throws DataSetException {
        super(new XmlProducer(new InputSource(reader)));
    }

    /**
     * Creates an XmlDataSet with the specified xml input stream.
     */
    public XmlDataSet(InputStream in) throws DataSetException {
        super(new XmlProducer(new InputSource(in)));
    }
}
