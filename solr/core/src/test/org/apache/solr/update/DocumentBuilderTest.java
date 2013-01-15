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
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.DocList;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.response.ResultContext;

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
  
  public void testMultiValuedFieldAndDocBoosts() throws Exception {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getSchema();
    SolrInputDocument doc = new SolrInputDocument();
    doc.setDocumentBoost(3.0f);
    SolrInputField field = new SolrInputField( "foo_t" );
    field.addValue( "summer time" , 1.0f );
    field.addValue( "in the city" , 5.0f ); // using boost
    field.addValue( "living is easy" , 1.0f );
    doc.put( field.getName(), field );

    Document out = DocumentBuilder.toDocument( doc, core.getSchema() );
    IndexableField[] outF = out.getFields( field.getName() );
    assertEquals("wrong number of field values",
                 3, outF.length);

    // since Lucene no longer has native documnt boosts, we should find
    // the doc boost multiplied into the boost o nthe first field value
    // all other field values should be 1.0f
    // (lucene will multiply all of the field boosts later)
    assertEquals(15.0f, outF[0].boost(), 0.0f);
    assertEquals(1.0f, outF[1].boost(), 0.0f);
    assertEquals(1.0f, outF[2].boost(), 0.0f);
    
  }

  public void testCopyFieldsAndFieldBoostsAndDocBoosts() throws Exception {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getSchema();
    SolrInputDocument doc = new SolrInputDocument();

    final float DOC_BOOST = 3.0F;
    doc.setDocumentBoost(DOC_BOOST);
    doc.addField("id", "42");

    SolrInputField inTitle = new SolrInputField( "title" );
    inTitle.addValue( "titleA" , 2.0F ); 
    inTitle.addValue( "titleB" , 7.0F ); 
    final float TITLE_BOOST = 2.0F * 7.0F;
    assertEquals(TITLE_BOOST, inTitle.getBoost(), 0.0F);
    doc.put( inTitle.getName(), inTitle );
    
    SolrInputField inFoo = new SolrInputField( "foo_t" );
    inFoo.addValue( "summer time" , 1.0F );
    inFoo.addValue( "in the city" , 5.0F ); 
    inFoo.addValue( "living is easy" , 11.0F );
    final float FOO_BOOST = 1.0F * 5.0F * 11.0F;
    assertEquals(FOO_BOOST, inFoo.getBoost(), 0.0F);
    doc.put( inFoo.getName(), inFoo );

    Document out = DocumentBuilder.toDocument( doc, core.getSchema() );

    IndexableField[] outTitle = out.getFields( inTitle.getName() );
    assertEquals("wrong number of title values",
                 2, outTitle.length);

    IndexableField[] outNoNorms = out.getFields( "title_stringNoNorms" );
    assertEquals("wrong number of nonorms values",
                 2, outNoNorms.length);

    IndexableField[] outFoo = out.getFields( inFoo.getName() );
    assertEquals("wrong number of foo values",
                 3, outFoo.length);

    IndexableField[] outText = out.getFields( "text" );
    assertEquals("wrong number of text values",
                 5, outText.length);

    // since Lucene no longer has native document boosts, we should find
    // the doc boost multiplied into the boost on the first field value
    // of each field.  All other field values should be 1.0f
    // (lucene will multiply all of the field value boosts later)
    assertEquals(TITLE_BOOST * DOC_BOOST, outTitle[0].boost(), 0.0F);
    assertEquals(1.0F,                    outTitle[1].boost(), 0.0F);
    //
    assertEquals(FOO_BOOST * DOC_BOOST,   outFoo[0].boost(), 0.0F);
    assertEquals(1.0F,                    outFoo[1].boost(), 0.0F);
    assertEquals(1.0F,                    outFoo[2].boost(), 0.0F);
    //
    assertEquals(TITLE_BOOST * DOC_BOOST, outText[0].boost(), 0.0F);
    assertEquals(1.0F,                    outText[1].boost(), 0.0F);
    assertEquals(FOO_BOOST,               outText[2].boost(), 0.0F);
    assertEquals(1.0F,                    outText[3].boost(), 0.0F);
    assertEquals(1.0F,                    outText[4].boost(), 0.0F);
    
    // copyField dest with no norms should not have recieved any boost
    assertEquals(1.0F, outNoNorms[0].boost(), 0.0F);
    assertEquals(1.0F, outNoNorms[1].boost(), 0.0F);
    
    // now index that SolrInputDocument to check the computed norms

    assertU(adoc(doc));
    assertU(commit());

    SolrQueryRequest req = req("q", "id:42");
    try {
      // very hack-ish

      SolrQueryResponse rsp = new SolrQueryResponse();
      core.execute(core.getRequestHandler(req.getParams().get(CommonParams.QT)), req, rsp);

      DocList dl = ((ResultContext) rsp.getValues().get("response")).docs;
      assertTrue("can't find the doc we just added", 1 == dl.size());
      int docid = dl.iterator().nextDoc();

      SolrIndexSearcher searcher = req.getSearcher();
      AtomicReader reader = SlowCompositeReaderWrapper.wrap(searcher.getTopReaderContext().reader());

      assertTrue("similarity doesn't extend DefaultSimilarity, " + 
                 "config or defaults have changed since test was written",
                 searcher.getSimilarity() instanceof DefaultSimilarity);

      DefaultSimilarity sim = (DefaultSimilarity) searcher.getSimilarity();
      
      NumericDocValues titleNorms = reader.simpleNormValues("title");
      NumericDocValues fooNorms = reader.simpleNormValues("foo_t");
      NumericDocValues textNorms =  reader.simpleNormValues("text");

      assertEquals(expectedNorm(sim, 2, TITLE_BOOST * DOC_BOOST),
                   titleNorms.get(docid));

      assertEquals(expectedNorm(sim, 8-3, FOO_BOOST * DOC_BOOST),
                   fooNorms.get(docid));

      assertEquals(expectedNorm(sim, 2 + 8-3, 
                                TITLE_BOOST * FOO_BOOST * DOC_BOOST),
                   textNorms.get(docid));

    } finally {
      req.close();
    }
  }

  /**
   * Given a length, and boost returns the expected encoded norm 
   */
  private static byte expectedNorm(final DefaultSimilarity sim,
                                   final int length, final float boost) {
    
    return sim.encodeNormValue(boost / ((float) Math.sqrt(length)));

  }
    

  public void testBoostOmitNorms() throws Exception {
    XmlDoc xml = new XmlDoc();
    // explicitly boosting a field if that omits norms is not ok
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
    // boosting a field that is copied to another field that omits norms is ok
    xml.xml = "<doc>"
      + "<field name=\"id\">42</field>"
      + "<field name=\"title\" boost=\"3.0\">mytitle</field>"
      + "</doc>";
    assertNull(h.validateUpdate(add(xml, new String[0])));
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
