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
package org.apache.solr.handler;

import org.apache.solr.SolrTestCaseJ4;
import java.io.StringReader;
import java.util.Collection;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;

public class XmlUpdateRequestHandlerTest extends SolrTestCaseJ4 {
  private static XMLInputFactory inputFactory = XMLInputFactory.newInstance();
  protected static XmlUpdateRequestHandler handler;

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml","schema.xml");
    handler = new XmlUpdateRequestHandler();
  }

  @Test
  public void testReadDoc() throws Exception
  {
    String xml = 
      "<doc boost=\"5.5\">" +
      "  <field name=\"id\" boost=\"2.2\">12345</field>" +
      "  <field name=\"name\">kitten</field>" +
      "  <field name=\"cat\" boost=\"3\">aaa</field>" +
      "  <field name=\"cat\" boost=\"4\">bbb</field>" +
      "  <field name=\"cat\" boost=\"5\">bbb</field>" +
      "  <field name=\"ab\">a&amp;b</field>" +
      "</doc>";

    XMLStreamReader parser = 
      inputFactory.createXMLStreamReader( new StringReader( xml ) );
    parser.next(); // read the START document...
    //null for the processor is all right here
    XMLLoader loader = new XMLLoader(null, inputFactory);
    SolrInputDocument doc = loader.readDoc( parser );
    
    // Read boosts
    assertEquals( 5.5f, doc.getDocumentBoost() );
    assertEquals( 1.0f, doc.getField( "name" ).getBoost() );
    assertEquals( 2.2f, doc.getField( "id" ).getBoost() );
    // Boost is the product of each value
    assertEquals( (3*4*5.0f), doc.getField( "cat" ).getBoost() );
    
    // Read values
    assertEquals( "12345", doc.getField( "id" ).getValue() );
    assertEquals( "kitten", doc.getField( "name").getValue() );
    assertEquals( "a&b", doc.getField( "ab").getValue() ); // read something with escaped characters
    
    Collection<Object> out = doc.getField( "cat" ).getValues();
    assertEquals( 3, out.size() );
    assertEquals( "[aaa, bbb, bbb]", out.toString() );
  }
}
