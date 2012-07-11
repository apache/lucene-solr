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

package org.apache.solr.update;

import org.apache.lucene.document.Document;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
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
  
  @Test
  public void testCopyFieldWithDocumentBoost() {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getSchema();
    assertFalse(schema.getField("title").omitNorms());
    assertTrue(schema.getField("title_stringNoNorms").omitNorms());
    SolrInputDocument doc = new SolrInputDocument();
    doc.setDocumentBoost(3f);
    doc.addField( "title", "mytitle");
    Document out = DocumentBuilder.toDocument( doc, core.getSchema() );
    assertNotNull( out.get( "title_stringNoNorms" ) );
    assertTrue("title_stringNoNorms has the omitNorms attribute set to true, if the boost is different than 1.0, it will fail",1.0f == out.getField( "title_stringNoNorms" ).boost() );
    assertTrue("It is OK that title has a boost of 3",3.0f == out.getField( "title" ).boost() );
  }
  
  
  @Test
  public void testCopyFieldWithFieldBoost() {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getSchema();
    assertFalse(schema.getField("title").omitNorms());
    assertTrue(schema.getField("title_stringNoNorms").omitNorms());
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField( "title", "mytitle", 3.0f );
    Document out = DocumentBuilder.toDocument( doc, core.getSchema() );
    assertNotNull( out.get( "title_stringNoNorms" ) );
    assertTrue("title_stringNoNorms has the omitNorms attribute set to true, if the boost is different than 1.0, it will fail",1.0f == out.getField( "title_stringNoNorms" ).boost() );
    assertTrue("It is OK that title has a boost of 3",3.0f == out.getField( "title" ).boost() );
  }
  
  @Test
  public void testWithPolyFieldsAndFieldBoost() {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getSchema();
    assertFalse(schema.getField("store").omitNorms());
    assertTrue(schema.getField("store_0_coordinate").omitNorms());
    assertTrue(schema.getField("store_1_coordinate").omitNorms());
    assertFalse(schema.getField("amount").omitNorms());
    assertTrue(schema.getField("amount" + FieldType.POLY_FIELD_SEPARATOR + "_currency").omitNorms());
    assertTrue(schema.getField("amount" + FieldType.POLY_FIELD_SEPARATOR + "_amount_raw").omitNorms());
    
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField( "store", "40.7143,-74.006", 3.0f );
    doc.addField( "amount", "10.5", 3.0f );
    Document out = DocumentBuilder.toDocument( doc, core.getSchema() );
    assertNotNull( out.get( "store" ) );
    assertNotNull( out.get( "amount" ) );
    assertNotNull(out.getField("store_0_coordinate"));
    //NOTE: As the subtypes have omitNorm=true, they must have boost=1F, otherwise this is going to fail when adding the doc to Lucene.
    assertTrue(1f == out.getField("store_0_coordinate").boost());
    assertTrue(1f == out.getField("store_1_coordinate").boost());
    assertTrue(1f == out.getField("amount" + FieldType.POLY_FIELD_SEPARATOR + "_currency").boost());
    assertTrue(1f == out.getField("amount" + FieldType.POLY_FIELD_SEPARATOR + "_amount_raw").boost());
  }
  
  @Test
  public void testWithPolyFieldsAndDocumentBoost() {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getSchema();
    assertFalse(schema.getField("store").omitNorms());
    assertTrue(schema.getField("store_0_coordinate").omitNorms());
    assertTrue(schema.getField("store_1_coordinate").omitNorms());
    assertFalse(schema.getField("amount").omitNorms());
    assertTrue(schema.getField("amount" + FieldType.POLY_FIELD_SEPARATOR + "_currency").omitNorms());
    assertTrue(schema.getField("amount" + FieldType.POLY_FIELD_SEPARATOR + "_amount_raw").omitNorms());
    
    SolrInputDocument doc = new SolrInputDocument();
    doc.setDocumentBoost(3.0f);
    doc.addField( "store", "40.7143,-74.006");
    doc.addField( "amount", "10.5");
    Document out = DocumentBuilder.toDocument( doc, core.getSchema() );
    assertNotNull( out.get( "store" ) );
    assertNotNull(out.getField("store_0_coordinate"));
    //NOTE: As the subtypes have omitNorm=true, they must have boost=1F, otherwise this is going to fail when adding the doc to Lucene.
    assertTrue(1f == out.getField("store_0_coordinate").boost());
    assertTrue(1f == out.getField("store_1_coordinate").boost());
    assertTrue(1f == out.getField("amount" + FieldType.POLY_FIELD_SEPARATOR + "_currency").boost());
    assertTrue(1f == out.getField("amount" + FieldType.POLY_FIELD_SEPARATOR + "_amount_raw").boost());
  }
  
  /**
   * Its ok to boost a field if it has norms
   */
  public void testBoost() throws Exception {
    XmlDoc xml = new XmlDoc();
    xml.xml = "<doc>"
        + "<field name=\"id\">0</field>"
        + "<field name=\"title\" boost=\"3.0\">mytitle</field>"
        + "</doc>";
    assertNull(h.validateUpdate(add(xml, new String[0])));
  }
  
  /**
   * Its not ok to boost a field if it omits norms
   */
  public void testBoostOmitNorms() throws Exception {
    XmlDoc xml = new XmlDoc();
    xml.xml = "<doc>"
        + "<field name=\"id\">ignore_exception</field>"
        + "<field name=\"title_stringNoNorms\" boost=\"3.0\">mytitle</field>"
        + "</doc>";
    try {
      assertNull(h.validateUpdate(add(xml, new String[0])));
      fail("didn't get expected exception for boosting omit norms field");
    } catch (SolrException expected) {
      // expected exception
    }
  }
  
  /**
   * Its ok to supply a document boost even if a field omits norms
   */
  public void testDocumentBoostOmitNorms() throws Exception {
    XmlDoc xml = new XmlDoc();
    xml.xml = "<doc boost=\"3.0\">"
        + "<field name=\"id\">2</field>"
        + "<field name=\"title_stringNoNorms\">mytitle</field>"
        + "</doc>";
    assertNull(h.validateUpdate(add(xml, new String[0])));
  }

}
