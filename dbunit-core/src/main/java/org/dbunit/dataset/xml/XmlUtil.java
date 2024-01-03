package org.dbunit.dataset.xml;

import java.io.File;
import java.io.StringReader;
import java.net.MalformedURLException;

import org.xml.sax.InputSource;

public class XmlUtil {

    public static InputSource buildInputSourceFromContent(String content) {
        return new InputSource(new StringReader(content));
    }

    public static InputSource buildInputSourceFromFile(String xmlFile) throws MalformedURLException {
        return new InputSource(new File(xmlFile).getAbsoluteFile().toURI().toURL().toString());
    }
}
