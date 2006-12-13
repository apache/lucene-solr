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

import javax.xml.xpath.XPathExpressionException;

import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.search.config.IndexSchemaField;
import org.apache.xerces.xni.XNIException;
import org.apache.xerces.xni.parser.XMLDocumentFilter;
import org.apache.xerces.xni.parser.XMLInputSource;
import org.apache.xerces.xni.parser.XMLParserConfiguration;
import org.cyberneko.html.HTMLConfiguration;
import org.cyberneko.html.filters.ElementRemover;
import org.cyberneko.html.filters.Writer;
import org.w3c.dom.Node;

/**
 * This ContentStrategy applies the path to the Indexable and retrieves the
 * plain string content from the returning node. All of the nodes text content
 * will cleaned from any html tags.
 * 
 * @author Simon Willnauer
 * 
 */
public class HTMLStrategy extends
        org.apache.lucene.gdata.search.analysis.ContentStrategy {
    private static final String REMOVE_SCRIPT = "script";

    private static final String CHAR_ENCODING = "UTF-8";

    protected HTMLStrategy(IndexSchemaField fieldConfiguration) {
        super(fieldConfiguration);

    }

    /**
     * @see org.apache.lucene.gdata.search.analysis.ContentStrategy#processIndexable(org.apache.lucene.gdata.search.analysis.Indexable)
     */
    @Override
    public void processIndexable(Indexable<? extends Node, ? extends ServerBaseEntry> indexable)
            throws NotIndexableException {
        String path = this.config.getPath();
        Node node = null;
        try {
            node = indexable.applyPath(path);
        } catch (XPathExpressionException e1) {
            throw new NotIndexableException("Can not apply path -- " + path);

        }
        if(node == null)
            throw new NotIndexableException("Could not retrieve content for schema field: "+this.config);
        StringReader contentReader = new StringReader(node.getTextContent());
        /*
         * remove all elements and script parts
         */
        ElementRemover remover = new ElementRemover();
        remover.removeElement(REMOVE_SCRIPT);
        StringWriter contentWriter = new StringWriter();
        Writer writer = new Writer(contentWriter, CHAR_ENCODING);
        XMLDocumentFilter[] filters = { remover, writer, };
        XMLParserConfiguration parser = new HTMLConfiguration();
        parser.setProperty("http://cyberneko.org/html/properties/filters",
                filters);
        XMLInputSource source = new XMLInputSource(null, null, null,
                contentReader, CHAR_ENCODING);
        try {
            parser.parse(source);
        } catch (XNIException e) {
            throw new NotIndexableException("Can not parse html -- ", e);

        } catch (IOException e) {
            throw new NotIndexableException("Can not parse html -- ", e);

        }
        this.content = contentWriter.toString();
    }

    

}
