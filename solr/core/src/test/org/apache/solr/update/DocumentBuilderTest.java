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

package org.apache.solr.update;

import org.apache.lucene.document.Document;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 *
 */
public class DocumentBuilderTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testBuildDocument() throws Exception 
  {
    SolrCore core = h.getCore();
    
    // undefined field
    try {
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField( "unknown field", 12345, 1.0f );
      DocumentBuilder.toDocument( doc, core.getSchema() );
      fail( "should throw an error" );
    }
    catch( SolrException ex ) {
      assertEquals( "should be bad request", 400, ex.code() );
    }
  }

  @Test
  public void testNullField() 
  {
    SolrCore core = h.getCore();
    
    // make sure a null value is not indexed
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField( "name", null, 1.0f );
    Document out = DocumentBuilder.toDocument( doc, core.getSchema() );
    assertNull( out.get( "name" ) );
  }

  @Test
  public void testExceptions() 
  {
    SolrCore core = h.getCore();
    
    // make sure a null value is not indexed
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField( "id", "123", 1.0f );
    doc.addField( "unknown", "something", 1.0f );
    try {
      DocumentBuilder.toDocument( doc, core.getSchema() );
      fail( "added an unknown field" );
    }
    catch( Exception ex ) {
      assertTrue( "should have document ID", ex.getMessage().indexOf( "doc=123" ) > 0 );
    }
    doc.remove( "unknown" );
    

    doc.addField( "weight", "not a number", 1.0f );
    try {
      DocumentBuilder.toDocument( doc, core.getSchema() );
      fail( "invalid 'float' field value" );
    }
    catch( Exception ex ) {
      assertTrue( "should have document ID", ex.getMessage().indexOf( "doc=123" ) > 0 );
      assertTrue( "cause is number format", ex.getCause() instanceof NumberFormatException );
    }
    
    // now make sure it is OK
    doc.setField( "weight", "1.34", 1.0f );
    DocumentBuilder.toDocument( doc, core.getSchema() );
  }

  @Test
  public void testMultiField() throws Exception {
    SolrCore core = h.getCore();

    // make sure a null value is not indexed
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField( "home", "2.2,3.3", 1.0f );
    Document out = DocumentBuilder.toDocument( doc, core.getSchema() );
    assertNotNull( out.get( "home" ) );//contains the stored value and term vector, if there is one
    assertNotNull( out.getField( "home_0" + FieldType.POLY_FIELD_SEPARATOR + "double" ) );
    assertNotNull( out.getField( "home_1" + FieldType.POLY_FIELD_SEPARATOR + "double" ) );
  }

}
