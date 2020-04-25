/*
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
package org.apache.solr.util;


import java.io.StringReader;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.apache.solr.SolrTestCase;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

public abstract class DOMUtilTestBase extends SolrTestCase {
  
  private DocumentBuilder builder;
  private static final XPathFactory xpathFactory = XPathFactory.newInstance();
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
  }

  public Node getNode( String xml, String path ) throws Exception {
    return getNode( getDocument(xml), path );
  }
  
  public Node getNode( Document doc, String path ) throws Exception {
    XPath xpath = xpathFactory.newXPath();
    return (Node)xpath.evaluate(path, doc, XPathConstants.NODE);
  }
  
  public Document getDocument( String xml ) throws Exception {
    return builder.parse(new InputSource(new StringReader(xml)));
  }
}
