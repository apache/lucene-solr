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

package org.apache.solr.common.util;

import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

import org.apache.lucene.util.LuceneTestCase;

public class DOMUtilTest extends LuceneTestCase {
  
  private DocumentBuilder builder;
  private static final XPathFactory xpathFactory = XPathFactory.newInstance();
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
  }
  
  public void testAddToNamedListPrimitiveTypes() throws Exception {
    NamedList<Object> namedList = new SimpleOrderedMap<Object>();
    DOMUtil.addToNamedList( getNode( "<str name=\"String\">STRING</str>", "/str" ), namedList, null );
    assertTypeAndValue( namedList, "String", "STRING" );
    DOMUtil.addToNamedList( getNode( "<int name=\"Integer\">100</int>", "/int" ), namedList, null );
    assertTypeAndValue( namedList, "Integer", Integer.valueOf( 100 ) );
    DOMUtil.addToNamedList( getNode( "<long name=\"Long\">200</long>", "/long" ), namedList, null );
    assertTypeAndValue( namedList, "Long", Long.valueOf( 200 ) );
    DOMUtil.addToNamedList( getNode( "<float name=\"Float\">300</float>", "/float" ), namedList, null );
    assertTypeAndValue( namedList, "Float", Float.valueOf( 300 ) );
    DOMUtil.addToNamedList( getNode( "<double name=\"Double\">400</double>", "/double" ), namedList, null );
    assertTypeAndValue( namedList, "Double", Double.valueOf( 400 ) );
    DOMUtil.addToNamedList( getNode( "<bool name=\"Boolean\">true</bool>", "/bool" ), namedList, null );
    assertTypeAndValue( namedList, "Boolean", true );
    DOMUtil.addToNamedList( getNode( "<bool name=\"Boolean\">on</bool>", "/bool" ), namedList, null );
    assertTypeAndValue( namedList, "Boolean", true );
    DOMUtil.addToNamedList( getNode( "<bool name=\"Boolean\">yes</bool>", "/bool" ), namedList, null );
    assertTypeAndValue( namedList, "Boolean", true );
    DOMUtil.addToNamedList( getNode( "<bool name=\"Boolean\">false</bool>", "/bool" ), namedList, null );
    assertTypeAndValue( namedList, "Boolean", false );
    DOMUtil.addToNamedList( getNode( "<bool name=\"Boolean\">off</bool>", "/bool" ), namedList, null );
    assertTypeAndValue( namedList, "Boolean", false );
    DOMUtil.addToNamedList( getNode( "<bool name=\"Boolean\">no</bool>", "/bool" ), namedList, null );
    assertTypeAndValue( namedList, "Boolean", false );
  }

  private void assertTypeAndValue( NamedList<Object> namedList, String key, Object value ) throws Exception {
    Object v = namedList.get( key );
    assertNotNull( v );
    assertEquals( key, v.getClass().getSimpleName() );
    assertEquals( value, v );
    namedList.remove( key );
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
