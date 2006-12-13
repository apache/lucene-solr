/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.gdata.search.analysis;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.google.gdata.data.ExtensionProfile;
import com.google.gdata.util.common.xml.XmlWriter;

/**
 * <tt>Indexable</tt> implementation using the W3C Dom API and JAXP XPath
 * engine
 * 
 * @author Simon Willnauer
 * @param <R> -
 *            a subtype of {@link org.w3c.dom.Node} returned by the applyPath
 *            method
 * @param <I> -
 *            a subtype of {@link org.apache.lucene.gdata.data.ServerBaseEntry}
 */
public class DomIndexable<R extends Node, I extends ServerBaseEntry> extends
        Indexable<R, I> {
    private final Document document;

    private final XPath xPath;

    /**
     * @param applyAble
     * @throws NotIndexableException
     */
    public DomIndexable(I applyAble) throws NotIndexableException {
        super(applyAble);
        if (this.applyAble.getServiceConfig() == null)
            throw new NotIndexableException("ServiceConfig is not set");
        try {
            this.document = buildDomDocument();
        } catch (ParserConfigurationException e) {
            throw new NotIndexableException("Can not create document builder",
                    e);

        } catch (IOException e) {
            throw new NotIndexableException(
                    "IO Exception occurred while building indexable", e);

        } catch (SAXException e) {
            throw new NotIndexableException("Can not parse entry", e);

        }
        this.xPath = XPathFactory.newInstance().newXPath();

    }

    private Document buildDomDocument() throws ParserConfigurationException,
            IOException, SAXException {
        StringWriter stringWriter = new StringWriter();
        ExtensionProfile profile = this.applyAble.getServiceConfig()
                .getExtensionProfile();
        if (profile == null)
            throw new IllegalStateException("ExtensionProfile is not set");
        XmlWriter writer = new XmlWriter(stringWriter);
        this.applyAble.generateAtom(writer, profile);
        DocumentBuilder builder = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder();
        return builder.parse(new InputSource(new StringReader(stringWriter
                .toString())));

    }

    /**
     * @see org.apache.lucene.gdata.search.analysis.Indexable#applyPath(java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public R applyPath(String expression) throws XPathExpressionException {

        return (R) this.xPath.evaluate(expression, this.document,
                XPathConstants.NODE);
    }

}
