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

package org.apache.solr.client.solrj;


import com.google.common.collect.Maps;
import junit.framework.Assert;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest.ACTION;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.LukeResponse;
import org.apache.solr.client.solrj.response.PivotField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.AnalysisParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * This should include tests against the example solr config
 * 
 * This lets us try various SolrServer implementations with the same tests.
 * 
 *
 * @since solr 1.3
 */
@SuppressSSL
abstract public class SolrExampleTests extends SolrExampleTestsBase
{
  private static Logger log = LoggerFactory.getLogger(SolrExampleTests.class);

  static {
    ignoreException("uniqueKey");
  }
  /**
   * query the example
   */
  @Test
  public void testExampleConfig() throws Exception
  {    
    SolrServer server = getSolrServer();
    
    // Empty the database...
    server.deleteByQuery( "*:*" );// delete everything!
    
    // Now add something...
    SolrInputDocument doc = new SolrInputDocument();
    String docID = "1112211111";
    doc.addField( "id", docID, 1.0f );
    doc.addField( "name", "my name!", 1.0f );
    
    Assert.assertEquals( null, doc.getField("foo") );
    Assert.assertTrue(doc.getField("name").getValue() != null );
        
    UpdateResponse upres = server.add( doc ); 
    // System.out.println( "ADD:"+upres.getResponse() );
    Assert.assertEquals(0, upres.getStatus());
    
    upres = server.commit( true, true );
    // System.out.println( "COMMIT:"+upres.getResponse() );
    Assert.assertEquals(0, upres.getStatus());
    
    upres = server.optimize( true, true );
    // System.out.println( "OPTIMIZE:"+upres.getResponse() );
    Assert.assertEquals(0, upres.getStatus());
    
    SolrQuery query = new SolrQuery();
    query.setQuery( "id:"+docID );
    QueryResponse response = server.query( query );
    
    Assert.assertEquals(docID, response.getResults().get(0).getFieldValue("id") );
    
    // Now add a few docs for facet testing...
    List<SolrInputDocument> docs = new ArrayList<>();
    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.addField( "id", "2", 1.0f );
    doc2.addField( "inStock", true, 1.0f );
    doc2.addField( "price", 2, 1.0f );
    doc2.addField( "timestamp_dt", new java.util.Date(), 1.0f );
    docs.add(doc2);
    SolrInputDocument doc3 = new SolrInputDocument();
    doc3.addField( "id", "3", 1.0f );
    doc3.addField( "inStock", false, 1.0f );
    doc3.addField( "price", 3, 1.0f );
    doc3.addField( "timestamp_dt", new java.util.Date(), 1.0f );
    docs.add(doc3);
    SolrInputDocument doc4 = new SolrInputDocument();
    doc4.addField( "id", "4", 1.0f );
    doc4.addField( "inStock", true, 1.0f );
    doc4.addField( "price", 4, 1.0f );
    doc4.addField( "timestamp_dt", new java.util.Date(), 1.0f );
    docs.add(doc4);
    SolrInputDocument doc5 = new SolrInputDocument();
    doc5.addField( "id", "5", 1.0f );
    doc5.addField( "inStock", false, 1.0f );
    doc5.addField( "price", 5, 1.0f );
    doc5.addField( "timestamp_dt", new java.util.Date(), 1.0f );
    docs.add(doc5);
    
    upres = server.add( docs ); 
    // System.out.println( "ADD:"+upres.getResponse() );
    Assert.assertEquals(0, upres.getStatus());
    
    upres = server.commit( true, true );
    // System.out.println( "COMMIT:"+upres.getResponse() );
    Assert.assertEquals(0, upres.getStatus());
    
    upres = server.optimize( true, true );
    // System.out.println( "OPTIMIZE:"+upres.getResponse() );
    Assert.assertEquals(0, upres.getStatus());
    
    query = new SolrQuery("*:*");
    query.addFacetQuery("price:[* TO 2]");
    query.addFacetQuery("price:[2 TO 4]");
    query.addFacetQuery("price:[5 TO *]");
    query.addFacetField("inStock");
    query.addFacetField("price");
    query.addFacetField("timestamp_dt");
    query.removeFilterQuery("inStock:true");
    
    response = server.query( query );
    Assert.assertEquals(0, response.getStatus());
    Assert.assertEquals(5, response.getResults().getNumFound() );
    Assert.assertEquals(3, response.getFacetQuery().size());    
    Assert.assertEquals(2, response.getFacetField("inStock").getValueCount());
    Assert.assertEquals(4, response.getFacetField("price").getValueCount());
    
    // test a second query, test making a copy of the main query
    SolrQuery query2 = query.getCopy();
    query2.addFilterQuery("inStock:true");
    response = server.query( query2 );
    Assert.assertEquals(1, query2.getFilterQueries().length);
    Assert.assertEquals(0, response.getStatus());
    Assert.assertEquals(2, response.getResults().getNumFound() );
    Assert.assertFalse(query.getFilterQueries() == query2.getFilterQueries());

    // sanity check round tripping of params...
    query = new SolrQuery("foo");
    query.addFilterQuery("{!field f=inStock}true");
    query.addFilterQuery("{!term f=name}hoss");
    query.addFacetQuery("price:[* TO 2]");
    query.addFacetQuery("price:[2 TO 4]");

    response = server.query( query );
    assertTrue("echoed params are not a NamedList: " +
               response.getResponseHeader().get("params").getClass(),
               response.getResponseHeader().get("params") instanceof NamedList);
    NamedList echo = (NamedList) response.getResponseHeader().get("params");
    List values = null;
    assertEquals("foo", echo.get("q"));
    assertTrue("echoed fq is not a List: " + echo.get("fq").getClass(),
               echo.get("fq") instanceof List);
    values = (List) echo.get("fq");
    Assert.assertEquals(2, values.size());
    Assert.assertEquals("{!field f=inStock}true", values.get(0));
    Assert.assertEquals("{!term f=name}hoss", values.get(1));
    assertTrue("echoed facet.query is not a List: " + 
               echo.get("facet.query").getClass(),
               echo.get("facet.query") instanceof List);
    values = (List) echo.get("facet.query");
    Assert.assertEquals(2, values.size());
    Assert.assertEquals("price:[* TO 2]", values.get(0));
    Assert.assertEquals("price:[2 TO 4]", values.get(1));
    
    
    if (jetty != null) {
      // check system wide system handler + "/admin/info/system"
      String url = jetty.getBaseUrl().toString();
      HttpSolrServer client = new HttpSolrServer(url);
      SolrQuery q = new SolrQuery();
      q.set("qt", "/admin/info/system");
      QueryResponse rsp = client.query(q);
      assertNotNull(rsp.getResponse().get("mode"));
      assertNotNull(rsp.getResponse().get("lucene"));
      client.shutdown();
    }
  }


  /**
   * query the example
   */
 @Test
 public void testAddRetrieve() throws Exception
  {    
    SolrServer server = getSolrServer();
    
    // Empty the database...
    server.deleteByQuery( "*:*" );// delete everything!
    
    // Now add something...
    SolrInputDocument doc1 = new SolrInputDocument();
    doc1.addField( "id", "id1", 1.0f );
    doc1.addField( "name", "doc1", 1.0f );
    doc1.addField( "price", 10 );

    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.addField( "id", "id2", 1.0f );
    doc2.addField( "name", "h\uD866\uDF05llo", 1.0f );
    doc2.addField( "price", 20 );
    
    Collection<SolrInputDocument> docs = new ArrayList<>();
    docs.add( doc1 );
    docs.add( doc2 );
    
    // Add the documents
    server.add( docs );
    server.commit();
    
    SolrQuery query = new SolrQuery();
    query.setQuery( "*:*" );
    query.addSortField( "price", SolrQuery.ORDER.asc );
    QueryResponse rsp = server.query( query );
    
    assertEquals( 2, rsp.getResults().getNumFound() );
    // System.out.println( rsp.getResults() );
    
    // Now do it again
    server.add( docs );
    server.commit();
    
    rsp = server.query( query );
    assertEquals( 2, rsp.getResults().getNumFound() );
    // System.out.println( rsp.getResults() );

    // query outside ascii range
    query.setQuery("name:h\uD866\uDF05llo");
    rsp = server.query( query );
    assertEquals( 1, rsp.getResults().getNumFound() );

  }
 

 /**
  * Get empty results
  */
  @Test
  public void testGetEmptyResults() throws Exception
  {    
    SolrServer server = getSolrServer();
     
    // Empty the database...
    server.deleteByQuery( "*:*" );// delete everything!
    server.commit();
     
    // Add two docs
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField( "id", "id1", 1.0f );
    doc.addField( "name", "doc1", 1.0f );
    doc.addField( "price", 10 );
    server.add( doc );
    
    doc = new SolrInputDocument();
    doc.addField( "id", "id2", 1.0f );
    server.add( doc );
    server.commit();
    
    // Make sure we get empty docs for unknown field
    SolrDocumentList out = server.query( new SolrQuery( "*:*" ).set("fl", "foofoofoo" ) ).getResults();
    assertEquals( 2, out.getNumFound() );
    assertEquals( 0, out.get(0).size() );
    assertEquals( 0, out.get(1).size() );

  }
  
  private String randomTestString(int maxLength) {
    // we can't just use _TestUtil.randomUnicodeString() or we might get 0xfffe etc
    // (considered invalid by XML)
    
    int size = random().nextInt(maxLength);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < size; i++) {
      switch(random().nextInt(4)) {
        case 0: /* single byte */ 
          sb.append('a'); 
          break;
        case 1: /* two bytes */
          sb.append('\u0645');
          break;
        case 2: /* three bytes */
          sb.append('\u092a');
          break;
        case 3: /* four bytes */
          sb.appendCodePoint(0x29B05);
      }
    }
    return sb.toString();
  }
  
  public void testUnicode() throws Exception {
    Random random = random();
    int numIterations = atLeast(3);
    
    SolrServer server = getSolrServer();
    
    // save the old parser, so we can set it back.
    ResponseParser oldParser = null;
    if (server instanceof HttpSolrServer) {
      HttpSolrServer cserver = (HttpSolrServer) server;
      oldParser = cserver.getParser();
    }
    
    try {
      for (int iteration = 0; iteration < numIterations; iteration++) {
        // choose format
        if (server instanceof HttpSolrServer) {
          if (random.nextBoolean()) {
            ((HttpSolrServer) server).setParser(new BinaryResponseParser());
          } else {
            ((HttpSolrServer) server).setParser(new XMLResponseParser());
          }
        }

        int numDocs = TestUtil.nextInt(random(), 1, 10 * RANDOM_MULTIPLIER);
        
        // Empty the database...
        server.deleteByQuery("*:*");// delete everything!
        
        List<SolrInputDocument> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
          // Now add something...
          SolrInputDocument doc = new SolrInputDocument();
          doc.addField("id", "" + i);
          doc.addField("unicode_s", randomTestString(30));
          docs.add(doc);
        }
        
        server.add(docs);
        server.commit();
        
        SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        query.setRows(numDocs);
        
        QueryResponse rsp = server.query( query );
        
        for (int i = 0; i < numDocs; i++) {
          String expected = (String) docs.get(i).getFieldValue("unicode_s");
          String actual = (String) rsp.getResults().get(i).getFieldValue("unicode_s");
          assertEquals(expected, actual);
        }
      }
    } finally {
      if (oldParser != null) {
        // set the old parser back
        ((HttpSolrServer)server).setParser(oldParser);
      }
    }
  }

  @Test
  public void testErrorHandling() throws Exception
  {    
    SolrServer server = getSolrServer();

    SolrQuery query = new SolrQuery();
    query.set(CommonParams.QT, "/analysis/field");
    query.set(AnalysisParams.FIELD_TYPE, "int");
    query.set(AnalysisParams.FIELD_VALUE, "ignore_exception");
    try {
      server.query( query );
      Assert.fail("should have a number format exception");
    }
    catch(SolrException ex) {
      assertEquals(400, ex.code());
      assertEquals("Invalid Number: ignore_exception", ex.getMessage());  // The reason should get passed through
    }
    catch(Throwable t) {
      t.printStackTrace();
      Assert.fail("should have thrown a SolrException! not: "+t);
    }
    
    try {
      //the df=text here is a kluge for the test to supply a default field in case there is none in schema.xml
      // alternatively, the resulting assertion could be modified to assert that no default field is specified.
      server.deleteByQuery( "{!df=text} ??::?? ignore_exception" ); // query syntax error
      Assert.fail("should have a number format exception");
    }
    catch(SolrException ex) {
      assertEquals(400, ex.code());
      assertTrue(ex.getMessage().indexOf("??::?? ignore_exception")>0);  // The reason should get passed through
    }
    catch(Throwable t) {
      t.printStackTrace();
      Assert.fail("should have thrown a SolrException! not: "+t);

    }
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "DOCID", 1.0f);
    doc.addField("id", "DOCID2", 1.0f);
    doc.addField("name", "hello", 1.0f);

    if (server instanceof HttpSolrServer) {
      try {
        server.add(doc);
        fail("Should throw exception!");
      } catch (SolrException ex) {
        assertEquals(400, ex.code());
        assertTrue(ex.getMessage().indexOf(
            "contains multiple values for uniqueKey") > 0); // The reason should get passed through
      } catch (Throwable t) {
        Assert.fail("should have thrown a SolrException! not: " + t);
      }
    } else if (server instanceof ConcurrentUpdateSolrServer) {
      //XXX concurrentupdatesolrserver reports errors differently
      ConcurrentUpdateSolrServer cs = (ConcurrentUpdateSolrServer) server;
      Field field = getCUSSExceptionField(cs);
      field.set(cs,  null);
      cs.add(doc);
      cs.blockUntilFinished();
      Throwable lastError = (Throwable)field.get(cs);
      assertNotNull("Should throw exception!", lastError); //XXX 
    } else {
      log.info("Ignorig update test for client:" + server.getClass().getName());
    }
  }
  
  private static Field getCUSSExceptionField(Object cs)
      throws SecurityException, NoSuchFieldException, IllegalArgumentException {
    Field field = cs.getClass().getDeclaredField("lastError");
    field.setAccessible(true);
    return field;
  }

  @Test
  public void testAugmentFields() throws Exception
  {    
    SolrServer server = getSolrServer();
    
    // Empty the database...
    server.deleteByQuery( "*:*" );// delete everything!
    
    // Now add something...
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField( "id", "111", 1.0f );
    doc.addField( "name", "doc1", 1.0f );
    doc.addField( "price", 11 );
    server.add( doc );
    server.commit(); // make sure this gets in first
    
    doc = new SolrInputDocument();
    doc.addField( "id", "222", 1.0f );
    doc.addField( "name", "doc2", 1.0f );
    doc.addField( "price", 22 );
    server.add( doc );
    server.commit();
    
    SolrQuery query = new SolrQuery();
    query.setQuery( "*:*" );
    query.set( CommonParams.FL, "id,price,[docid],[explain style=nl],score,aaa:[value v=aaa],ten:[value v=10 t=int]" );
    query.addSortField( "price", SolrQuery.ORDER.asc );
    QueryResponse rsp = server.query( query );
    
    SolrDocumentList out = rsp.getResults();
    assertEquals( 2, out.getNumFound() );
    SolrDocument out1 = out.get( 0 ); 
    SolrDocument out2 = out.get( 1 );
    assertEquals( "111", out1.getFieldValue( "id" ) );
    assertEquals( "222", out2.getFieldValue( "id" ) );
    assertEquals( 1.0f, out1.getFieldValue( "score" ) );
    assertEquals( 1.0f, out2.getFieldValue( "score" ) );
    
    // check that the docid is one bigger
    int id1 = (Integer)out1.getFieldValue( "[docid]" );
    int id2 = (Integer)out2.getFieldValue( "[docid]" );
    assertTrue( "should be bigger ["+id1+","+id2+"]", id2 > id1 );
    
    // The score from explain should be the same as the score
    NamedList explain = (NamedList)out1.getFieldValue( "[explain]" );
    assertEquals( out1.get( "score"), explain.get( "value" ) );
    
    // Augmented _value_ with alias
    assertEquals( "aaa", out1.get( "aaa" ) );
    assertEquals( 10, ((Integer)out1.get( "ten" )).intValue() );
  }

  @Test
  public void testUpdateRequestWithParameters() throws Exception {
    SolrServer server1 = createNewSolrServer();
    
    server1.deleteByQuery( "*:*" );
    server1.commit();
    
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "id1");
    
    UpdateRequest req = new UpdateRequest();
    req.setParam("overwrite", "false");
    req.add(doc);
    server1.request(req);
    server1.request(req);
    server1.commit();
    
    SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    QueryResponse rsp = server1.query(query);
    
    SolrDocumentList out = rsp.getResults();
    assertEquals(2, out.getNumFound());
    if (!(server1 instanceof EmbeddedSolrServer)) {
      /* Do not close in case of using EmbeddedSolrServer,
       * as that would close the CoreContainer */
      server1.shutdown();
    }
  }
  
 @Test
 public void testContentStreamRequest() throws Exception {
    SolrServer server = getSolrServer();
    server.deleteByQuery( "*:*" );// delete everything!
    server.commit();
    QueryResponse rsp = server.query( new SolrQuery( "*:*") );
    Assert.assertEquals( 0, rsp.getResults().getNumFound() );

    ContentStreamUpdateRequest up = new ContentStreamUpdateRequest("/update");
    up.addFile(getFile("solrj/books.csv"), "application/csv");
    up.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
    NamedList<Object> result = server.request(up);
    assertNotNull("Couldn't upload books.csv", result);
    rsp = server.query( new SolrQuery( "*:*") );
    Assert.assertEquals( 10, rsp.getResults().getNumFound() );
 }

 @Test
 public void testMultiContentStreamRequest() throws Exception {
    SolrServer server = getSolrServer();
    server.deleteByQuery( "*:*" );// delete everything!
    server.commit();
    QueryResponse rsp = server.query( new SolrQuery( "*:*") );
    Assert.assertEquals( 0, rsp.getResults().getNumFound() );

    ContentStreamUpdateRequest up = new ContentStreamUpdateRequest("/update");
    up.addFile(getFile("solrj/docs1.xml"),"application/xml"); // 2
    up.addFile(getFile("solrj/docs2.xml"),"application/xml"); // 3
    up.setParam("a", "\u1234");
    up.setParam(CommonParams.HEADER_ECHO_PARAMS, CommonParams.EchoParamStyle.ALL.toString());
    up.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
    NamedList<Object> result = server.request(up);
    Assert.assertEquals("\u1234",
        ((NamedList)((NamedList) result.get("responseHeader")).get("params")).get("a"));
    assertNotNull("Couldn't upload xml files", result);
    rsp = server.query( new SolrQuery( "*:*") );
    Assert.assertEquals( 5 , rsp.getResults().getNumFound() );
  }
  
 @Test
 public void testLukeHandler() throws Exception
  {    
    SolrServer server = getSolrServer();
    
    // Empty the database...
    server.deleteByQuery( "*:*" );// delete everything!
    
    SolrInputDocument[] doc = new SolrInputDocument[5];
    for( int i=0; i<doc.length; i++ ) {
      doc[i] = new SolrInputDocument();
      doc[i].setField( "id", "ID"+i, 1.0f );
      server.add( doc[i] );
    }
    server.commit();
    assertNumFound( "*:*", doc.length ); // make sure it got in
    
    LukeRequest luke = new LukeRequest();
    luke.setShowSchema( false );
    LukeResponse rsp = luke.process( server );
    assertNull( rsp.getFieldTypeInfo() ); // if you don't ask for it, the schema is null
    
    luke.setShowSchema( true );
    rsp = luke.process( server );
    assertNotNull( rsp.getFieldTypeInfo() ); 
  }

 @Test
 public void testStatistics() throws Exception
  {    
    SolrServer server = getSolrServer();
    
    // Empty the database...
    server.deleteByQuery( "*:*" );// delete everything!
    server.commit();
    assertNumFound( "*:*", 0 ); // make sure it got in

    String f = "val_i";
    
    int i=0;               // 0   1   2   3   4   5   6   7   8   9 
    int[] nums = new int[] { 23, 26, 38, 46, 55, 63, 77, 84, 92, 94 };
    for( int num : nums ) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField( "id", "doc"+i++ );
      doc.setField( "name", "doc: "+num );
      doc.setField( f, num );
      server.add( doc );
    }
    server.commit();
    assertNumFound( "*:*", nums.length ); // make sure they all got in
    
    SolrQuery query = new SolrQuery( "*:*" );
    query.setRows( 0 );
    query.setGetFieldStatistics( f );
    
    QueryResponse rsp = server.query( query );
    FieldStatsInfo stats = rsp.getFieldStatsInfo().get( f );
    assertNotNull( stats );
    
    assertEquals( 23.0, ((Double)stats.getMin()).doubleValue(), 0 );
    assertEquals( 94.0, ((Double)stats.getMax()).doubleValue(), 0 );
    assertEquals( new Long(nums.length), stats.getCount() );
    assertEquals( new Long(0), stats.getMissing() );
    assertEquals( "26.4", stats.getStddev().toString().substring(0,4) );
    
    // now lets try again with a new set...  (odd median)
    //----------------------------------------------------
    server.deleteByQuery( "*:*" );// delete everything!
    server.commit();
    assertNumFound( "*:*", 0 ); // make sure it got in
    nums = new int[] { 5, 7, 10, 19, 20 };
    for( int num : nums ) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField( "id", "doc"+i++ );
      doc.setField( "name", "doc: "+num );
      doc.setField( f, num );
      server.add( doc );
    }
    server.commit();
    assertNumFound( "*:*", nums.length ); // make sure they all got in
    
    rsp = server.query( query );
    stats = rsp.getFieldStatsInfo().get( f );
    assertNotNull( stats );
    
    assertEquals( 5.0, ((Double)stats.getMin()).doubleValue(), 0 );
    assertEquals( 20.0, ((Double)stats.getMax()).doubleValue(), 0 );
    assertEquals( new Long(nums.length), stats.getCount() );
    assertEquals( new Long(0), stats.getMissing() );
    
    // Now try again with faceting
    //---------------------------------
    server.deleteByQuery( "*:*" );// delete everything!
    server.commit();
    assertNumFound( "*:*", 0 ); // make sure it got in
    nums = new int[] { 1, 2, 3, 4, 5, 10, 11, 12, 13, 14 };
    for( i=0; i<nums.length; i++ ) {
      int num = nums[i];
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField( "id", "doc"+i );
      doc.setField( "name", "doc: "+num );
      doc.setField( f, num );
      doc.setField( "inStock", i < 5 );
      server.add( doc );
    }
    server.commit();
    assertNumFound( "inStock:true",  5 ); // make sure they all got in
    assertNumFound( "inStock:false", 5 ); // make sure they all got in

    // facet on 'inStock'
    query.addStatsFieldFacets( f, "inStock" );
    rsp = server.query( query );
    stats = rsp.getFieldStatsInfo().get( f );
    assertNotNull( stats );
    
    List<FieldStatsInfo> facets = stats.getFacets().get( "inStock" );
    assertNotNull( facets );
    assertEquals( 2, facets.size() );
    FieldStatsInfo inStockF = facets.get(0);
    FieldStatsInfo inStockT = facets.get(1);
    if( "true".equals( inStockF.getName() ) ) {
      FieldStatsInfo tmp = inStockF;
      inStockF = inStockT;
      inStockT = tmp;
    }

    // make sure half went to each
    assertEquals( inStockF.getCount(), inStockT.getCount() );
    assertEquals( stats.getCount().longValue(), inStockF.getCount()+inStockT.getCount() );

    assertTrue( "check that min max faceted ok", ((Double)inStockF.getMin()).doubleValue() < ((Double)inStockF.getMax()).doubleValue() );
    assertEquals( "they have the same distribution", inStockF.getStddev(), inStockT.getStddev() );
  }

  @Test
  public void testPingHandler() throws Exception
  {    
    SolrServer server = getSolrServer();
    
    // Empty the database...
    server.deleteByQuery( "*:*" );// delete everything!
    server.commit();
    assertNumFound( "*:*", 0 ); // make sure it got in
    
    // should be ok
    server.ping();
    
  }
  
  @Test
  public void testFaceting() throws Exception
  {    
    SolrServer server = getSolrServer();
    
    // Empty the database...
    server.deleteByQuery( "*:*" );// delete everything!
    server.commit();
    assertNumFound( "*:*", 0 ); // make sure it got in
    
    ArrayList<SolrInputDocument> docs = new ArrayList<>(10);
    for( int i=1; i<=10; i++ ) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField( "id", i+"", 1.0f );
      if( (i%2)==0 ) {
        doc.addField( "features", "two" );
      }
      if( (i%3)==0 ) {
        doc.addField( "features", "three" );
      }
      if( (i%4)==0 ) {
        doc.addField( "features", "four" );
      }
      if( (i%5)==0 ) {
        doc.addField( "features", "five" );
      }
      docs.add( doc );
    }
    server.add( docs );
    server.commit();
    
    SolrQuery query = new SolrQuery( "*:*" );
    query.remove( FacetParams.FACET_FIELD );
    query.addFacetField( "features" );
    query.setFacetMinCount( 0 );
    query.setFacet( true );
    query.setRows( 0 );
    
    QueryResponse rsp = server.query( query );
    assertEquals( docs.size(), rsp.getResults().getNumFound() );
    
    List<FacetField> facets = rsp.getFacetFields();
    assertEquals( 1, facets.size() );
    FacetField ff = facets.get( 0 );
    assertEquals( "features", ff.getName() );
    // System.out.println( "111: "+ff.getValues() );
    // check all counts
    assertEquals( "[two (5), three (3), five (2), four (2)]", ff.getValues().toString() );
    
    // should be the same facets with minCount=0
    query.setFilterQueries( "features:two" );
    rsp = server.query( query );
    ff = rsp.getFacetField( "features" );
    assertEquals( "[two (5), four (2), five (1), three (1)]", ff.getValues().toString() );
    
    // with minCount > 3
    query.setFacetMinCount( 4 );
    rsp = server.query( query );
    ff = rsp.getFacetField( "features" );
    assertEquals( "[two (5)]", ff.getValues().toString() );

    // with minCount > 3
    query.setFacetMinCount( -1 );
    rsp = server.query( query );
    ff = rsp.getFacetField( "features" );
    
    // System.out.println( rsp.getResults().getNumFound() + " :::: 444: "+ff.getValues() );
  }

  @Test
  public void testPivotFacets() throws Exception {
    doPivotFacetTest(false);
  }
    
  public void testPivotFacetsMissing() throws Exception {
    doPivotFacetTest(true);
  }
    
  private void doPivotFacetTest(boolean missing) throws Exception {
    SolrServer server = getSolrServer();
    
    // Empty the database...
    server.deleteByQuery( "*:*" );// delete everything!
    server.commit();
    assertNumFound( "*:*", 0 ); // make sure it got in
    
    int id = 1;
    ArrayList<SolrInputDocument> docs = new ArrayList<>();
    docs.add( makeTestDoc( "id", id++, "features", "aaa",  "cat", "a", "inStock", true  ) );
    docs.add( makeTestDoc( "id", id++, "features", "aaa",  "cat", "a", "inStock", false ) );
    docs.add( makeTestDoc( "id", id++, "features", "aaa",  "cat", "a", "inStock", true ) );
    docs.add( makeTestDoc( "id", id++, "features", "aaa",  "cat", "b", "inStock", false ) );
    docs.add( makeTestDoc( "id", id++, "features", "aaa",  "cat", "b", "inStock", true ) );
    docs.add( makeTestDoc( "id", id++, "features", "bbb",  "cat", "a", "inStock", false ) );
    docs.add( makeTestDoc( "id", id++, "features", "bbb",  "cat", "a", "inStock", true ) );
    docs.add( makeTestDoc( "id", id++, "features", "bbb",  "cat", "b", "inStock", false ) );
    docs.add( makeTestDoc( "id", id++, "features", "bbb",  "cat", "b", "inStock", true ) );
    docs.add( makeTestDoc( "id", id++, "features", "bbb",  "cat", "b", "inStock", false ) );
    docs.add( makeTestDoc( "id", id++, "features", "bbb",  "cat", "b", "inStock", true ) );
    docs.add( makeTestDoc( "id", id++,  "cat", "b" ) ); // something not matching all fields
    server.add( docs );
    server.commit();
    
    SolrQuery query = new SolrQuery( "*:*" );
    query.addFacetPivotField("features,cat", "cat,features", "features,cat,inStock" );
    query.setFacetMinCount( 0 );
    query.setFacetMissing( missing );
    query.setRows( 0 );
    
    QueryResponse rsp = server.query( query );
    assertEquals( docs.size(), rsp.getResults().getNumFound() );
    
    NamedList<List<PivotField>> pivots = rsp.getFacetPivot();
    assertEquals( 3, pivots.size() );

//    for(Map.Entry<String, List<PivotField>> entry : pivots ) {
//      System.out.println( "PIVOT: "+entry.getKey() );
//      for( PivotField p : entry.getValue() ) {
//        p.write(System.out, 0 );
//      }
//      System.out.println();
//    }
    
    //  PIVOT: features,cat
    //  features=bbb (6)
    //    cat=b (4)
    //    cat=a (2)
    //  features=aaa (5)
    //    cat=a (3)
    //    cat=b (2)
    //  features missing (1)
    //    cat=b (1)

    assertEquals( "features,cat", pivots.getName( 0 ) );
    List<PivotField> pivot = pivots.getVal( 0 );
    assertEquals( missing ? 3 : 2, pivot.size() );
    
    PivotField ff = pivot.get( 0 );
    assertEquals( "features", ff.getField() );
    assertEquals( "bbb", ff.getValue() );
    assertEquals( 6, ff.getCount() );
    List<PivotField> counts = ff.getPivot();
    assertEquals( 2, counts.size() );
    assertEquals( "cat", counts.get(0).getField() );
    assertEquals( "b", counts.get(0).getValue() );
    assertEquals(   4, counts.get(0).getCount() );
    assertEquals( "a", counts.get(1).getValue() );
    assertEquals(   2, counts.get(1).getCount() );

    ff = pivot.get( 1 );
    assertEquals( "features", ff.getField() );
    assertEquals( "aaa", ff.getValue() );
    assertEquals( 5, ff.getCount() );
    counts = ff.getPivot();
    assertEquals( 2, counts.size() );
    assertEquals( "cat", counts.get(0).getField() );
    assertEquals( "a", counts.get(0).getValue() );
    assertEquals(   3, counts.get(0).getCount() );
    assertEquals( "b", counts.get(1).getValue() );
    assertEquals(   2, counts.get(1).getCount() );

    if (missing) {
      ff = pivot.get( 2 );
      assertEquals( "features", ff.getField() );
      assertEquals( null, ff.getValue() );
      assertEquals( 1, ff.getCount() );
      counts = ff.getPivot();
      assertEquals( 1, counts.size() );
      assertEquals( "cat", counts.get(0).getField() );
      assertEquals( "b", counts.get(0).getValue() );
      assertEquals( 1, counts.get(0).getCount() );
    }

    //  PIVOT: cat,features
    //  cat=b (7)
    //    features=bbb (4)
    //    features=aaa (2)
    //    features missing (1)
    //  cat=a (5)
    //    features=aaa (3)
    //    features=bbb (2)

    assertEquals( "cat,features", pivots.getName( 1 ) );
    pivot = pivots.getVal( 1 );
    assertEquals( 2, pivot.size() );

    ff = pivot.get( 0 );
    assertEquals( "cat", ff.getField() );
    assertEquals( "b", ff.getValue() );
    assertEquals( 7, ff.getCount() );
    counts = ff.getPivot();
    assertEquals( missing ? 3 : 2, counts.size() );
    assertEquals( "features", counts.get(0).getField() );
    assertEquals( "bbb", counts.get(0).getValue() );
    assertEquals( 4, counts.get(0).getCount() );
    assertEquals( "aaa", counts.get(1).getValue() );
    assertEquals( 2, counts.get(1).getCount() );
    if ( missing ) {
      assertEquals( null, counts.get(2).getValue() );
      assertEquals( 1, counts.get(2).getCount() );
    }

    ff = pivot.get( 1 );
    assertEquals( "cat", ff.getField() );
    assertEquals( "a", ff.getValue() );
    assertEquals( 5, ff.getCount() );
    counts = ff.getPivot();
    assertEquals( 2, counts.size() );
    assertEquals( "features", counts.get(0).getField() );
    assertEquals( "aaa", counts.get(0).getValue() );
    assertEquals( 3, counts.get(0).getCount() );
    assertEquals( "bbb", counts.get(1).getValue() );
    assertEquals( 2, counts.get(1).getCount() );

    // Three deep:
    //  PIVOT: features,cat,inStock
    //  features=bbb (6)
    //    cat=b (4)
    //      inStock=false (2)
    //      inStock=true (2)
    //    cat=a (2)
    //      inStock=false (1)
    //      inStock=true (1)
    //  features=aaa (5)
    //    cat=a (3)
    //      inStock=true (2)
    //      inStock=false (1)
    //    cat=b (2)
    //      inStock=false (1)
    //      inStock=true (1)
    //  features missing (1)
    //    cat=b (1)
    //      inStock missing (1)

    assertEquals( "features,cat,inStock", pivots.getName( 2 ) );
    pivot = pivots.getVal( 2 );
    assertEquals( missing ? 3 : 2, pivot.size() );
    PivotField p = pivot.get( 1 ).getPivot().get(0);     // get(1) should be features=AAAA, then get(0) should be cat=a
    assertEquals( "cat", p.getField() );
    assertEquals( "a", p.getValue() );
    counts = p.getPivot();
  //  p.write(System.out, 5 );
    assertEquals( 2, counts.size() );  // 2 trues and 1 false under features=AAAA,cat=a
    assertEquals( "inStock",    counts.get(0).getField() );
    assertEquals( Boolean.TRUE, counts.get(0).getValue() );
    assertEquals(  2,           counts.get(0).getCount() );

    if (missing) {
      p = pivot.get( 2 );
      assertEquals( "features", p.getField() );
      assertEquals( null, p.getValue() );
      assertEquals( 1, p.getCount() );
      assertEquals( 1, p.getPivot().size() );
      p = p.getPivot().get(0);
      assertEquals( "cat", p.getField() );
      assertEquals( "b", p.getValue() );
      assertEquals( 1, p.getCount() );
      assertEquals( 1, p.getPivot().size() );
      p = p.getPivot().get(0);
      assertEquals( "inStock", p.getField() );
      assertEquals( null, p.getValue() );
      assertEquals( 1, p.getCount() );
      assertEquals( null, p.getPivot() );
    }

    // -- SOLR-2255 Test excluding a filter Query --
    // this test is a slight modification to the first pivot facet test
    query = new SolrQuery( "*:*" );
    query.addFacetPivotField( "{!ex=mytag key=mykey}features,cat" );
    query.addFilterQuery("{!tag=mytag}-(features:bbb AND cat:a AND inStock:true)");//filters out one
    query.setFacetMinCount( 0 );
    query.setRows( 0 );

    rsp = server.query( query );
    assertEquals( docs.size() - 1, rsp.getResults().getNumFound() );//one less due to filter

    //The rest of this test should be just like the original since we've
    // excluded the 'fq' from the facet count
    pivots = rsp.getFacetPivot();
    pivot = pivots.getVal(0);
    assertEquals( "mykey", pivots.getName( 0 ) );
    assertEquals( 2, pivot.size() );

    ff = pivot.get( 0 );
    assertEquals( "features", ff.getField() );
    assertEquals( "bbb", ff.getValue() );
    assertEquals( 6, ff.getCount() );
    counts = ff.getPivot();
    assertEquals( 2, counts.size() );
    assertEquals( "cat", counts.get(0).getField() );
    assertEquals( "b", counts.get(0).getValue() );
    assertEquals(   4, counts.get(0).getCount() );
    assertEquals( "a", counts.get(1).getValue() );
    assertEquals(   2, counts.get(1).getCount() );

  }
  
  public static SolrInputDocument makeTestDoc( Object ... kvp )
  {
    SolrInputDocument doc = new SolrInputDocument();
    for( int i=0; i<kvp.length; ) {
      String k = (String)kvp[i++];
      Object v = kvp[i++];
      doc.addField( k, v );
    }
    return doc;
  }

  @Test
  public void testChineseDefaults() throws Exception {
    SolrServer server = getSolrServer();
    // Empty the database...
    server.deleteByQuery( "*:*" );// delete everything!
    server.commit();
    assertNumFound( "*:*", 0 ); // make sure it got in

    // Beijing medical University
    UpdateRequest req = new UpdateRequest();
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "42");
    doc.addField("text", "北京医科大学");
    req.add(doc);

    req.setAction(ACTION.COMMIT, true, true );
    req.process( server );

    // Beijing university should match:
    SolrQuery query = new SolrQuery("北京大学");
    QueryResponse rsp = server.query( query );
    assertEquals(1, rsp.getResults().getNumFound());
  }

  @Test
  public void testRealtimeGet() throws Exception
  {    
    SolrServer server = getSolrServer();
    
    // Empty the database...
    server.deleteByQuery( "*:*" );// delete everything!
    
    // Now add something...
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField( "id", "DOCID", 1.0f );
    doc.addField( "name", "hello", 1.0f );
    server.add( doc );
    server.commit();  // Since the transaction log is disabled in the example, we need to commit
    
    SolrQuery q = new SolrQuery();
    q.setRequestHandler("/get");
    q.set("id", "DOCID");
    q.set("fl", "id,name,aaa:[value v=aaa]");
    
    // First Try with the BinaryResponseParser
    QueryRequest req = new QueryRequest( q );
    req.setResponseParser(new BinaryResponseParser());
    QueryResponse rsp = req.process(server);
    SolrDocument out = (SolrDocument)rsp.getResponse().get("doc");
    assertEquals("DOCID", out.get("id"));
    assertEquals("hello", out.get("name"));
    assertEquals("aaa", out.get("aaa"));

    // Then with the XMLResponseParser
    req.setResponseParser(new XMLResponseParser());
    rsp = req.process(server);
    out = (SolrDocument)rsp.getResponse().get("doc");
    assertEquals("DOCID", out.get("id"));
    assertEquals("hello", out.get("name"));
    assertEquals("aaa", out.get("aaa"));
  }
  
  @Test
  public void testUpdateField() throws Exception {
    //no versions
    SolrServer server = getSolrServer();
    server.deleteByQuery("*:*");
    server.commit();
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "unique");
    doc.addField("name", "gadget");
    doc.addField("price_f", 1);
    server.add(doc);
    server.commit();
    SolrQuery q = new SolrQuery("*:*");
    q.setFields("id","price_f","name", "_version_");
    QueryResponse resp = server.query(q);
    assertEquals("Doc count does not match", 1, resp.getResults().getNumFound());
    Long version = (Long)resp.getResults().get(0).getFirstValue("_version_");
    assertNotNull("no version returned", version);
    assertEquals(1.0f, resp.getResults().get(0).getFirstValue("price_f"));

    //update "price" with incorrect version (optimistic locking)
    HashMap<String, Object> oper = new HashMap<>();  //need better api for this???
    oper.put("set",100);

    doc = new SolrInputDocument();
    doc.addField("id", "unique");
    doc.addField("_version_", version+1);
    doc.addField("price_f", oper);
    try {
      server.add(doc);
      if(server instanceof HttpSolrServer) { //XXX concurrent server reports exceptions differently
        fail("Operation should throw an exception!");
      } else {
        server.commit(); //just to be sure the client has sent the doc
        assertTrue("CUSS did not report an error", ((Throwable)getCUSSExceptionField(server).get(server)).getMessage().contains("Conflict"));
      }
    } catch (SolrException se) {
      assertTrue("No identifiable error message", se.getMessage().contains("version conflict for unique"));
    }
    
    //update "price", use correct version (optimistic locking)
    doc = new SolrInputDocument();
    doc.addField("id", "unique");
    doc.addField("_version_", version);
    doc.addField("price_f", oper);
    server.add(doc);
    server.commit();
    resp = server.query(q);
    assertEquals("Doc count does not match", 1, resp.getResults().getNumFound());
    assertEquals("price was not updated?", 100.0f, resp.getResults().get(0).getFirstValue("price_f"));
    assertEquals("no name?", "gadget", resp.getResults().get(0).getFirstValue("name"));

    //update "price", no version
    oper.put("set", 200);
    doc = new SolrInputDocument();
    doc.addField("id", "unique");
    doc.addField("price_f", oper);
    server.add(doc);
    server.commit();
    resp = server.query(q);
    assertEquals("Doc count does not match", 1, resp.getResults().getNumFound());
    assertEquals("price was not updated?", 200.0f, resp.getResults().get(0).getFirstValue("price_f"));
    assertEquals("no name?", "gadget", resp.getResults().get(0).getFirstValue("name"));
  }

  @Test
  public void testUpdateMultiValuedField() throws Exception {
    SolrServer solrServer = getSolrServer();
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "123");
    solrServer.add(doc);
    solrServer.commit(true, true);
    QueryResponse response = solrServer.query(new SolrQuery("id:123"));
    assertEquals("Failed to add doc to cloud server", 1, response.getResults().getNumFound());

    Map<String, List<String>> operation = new HashMap<>();
    operation.put("set", Arrays.asList("first", "second", "third"));
    doc.addField("multi_ss", operation);
    solrServer.add(doc);
    solrServer.commit(true, true);
    response = solrServer.query(new SolrQuery("id:123"));
    assertTrue("Multi-valued field did not return a collection", response.getResults().get(0).get("multi_ss") instanceof List);
    List<String> values = (List<String>) response.getResults().get(0).get("multi_ss");
    assertEquals("Field values was not updated with all values via atomic update", 3, values.size());

    operation.clear();
    operation.put("add", Arrays.asList("fourth", "fifth"));
    doc.removeField("multi_ss");
    doc.addField("multi_ss", operation);
    solrServer.add(doc);
    solrServer.commit(true, true);
    response = solrServer.query(new SolrQuery("id:123"));
    values = (List<String>) response.getResults().get(0).get("multi_ss");
    assertEquals("Field values was not updated with all values via atomic update", 5, values.size());
  }

  @Test
  public void testSetNullUpdates() throws Exception {
    SolrServer solrServer = getSolrServer();
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "testSetNullUpdates");
    doc.addField("single_s", "test-value");
    doc.addField("multi_ss", Arrays.asList("first", "second"));
    solrServer.add(doc);
    solrServer.commit(true, true);
    doc.removeField("single_s");
    doc.removeField("multi_ss");
    Map<String, Object> map = Maps.newHashMap();
    map.put("set", null);
    doc.addField("multi_ss", map);
    solrServer.add(doc);
    solrServer.commit(true, true);
    QueryResponse response = solrServer.query(new SolrQuery("id:testSetNullUpdates"));
    assertNotNull("Entire doc was replaced because null update was not written", response.getResults().get(0).getFieldValue("single_s"));
    assertNull("Null update failed. Value still exists in document", response.getResults().get(0).getFieldValue("multi_ss"));
  }

  public void testSetNullUpdateOrder() throws Exception {
    SolrServer solrServer = getSolrServer();
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "testSetNullUpdateOrder");
    doc.addField("single_s", "test-value");
    doc.addField("multi_ss", Arrays.asList("first", "second"));
    solrServer.add(doc);
    solrServer.commit(true, true);

    Map<String, Object> map = Maps.newHashMap();
    map.put("set", null);
    doc = new SolrInputDocument();
    doc.addField("multi_ss", map);
    doc.addField("id", "testSetNullUpdateOrder");
    doc.addField("single_s", "test-value2");
    solrServer.add(doc);
    solrServer.commit();

    QueryResponse response = solrServer.query(new SolrQuery("id:testSetNullUpdateOrder"));
    assertEquals("Field included after set null=true not updated via atomic update", "test-value2",
        response.getResults().get(0).getFieldValue("single_s"));
  }
  
  @Test
  public void testQueryWithParams() throws SolrServerException {
    SolrServer server = getSolrServer();
    SolrQuery q = new SolrQuery("query");
    q.setParam("debug", true);
    QueryResponse resp = server.query(q);
    assertEquals(
        "server didn't respond with debug=true, didn't we pass in the parameter?",
        "true",
        ((NamedList) resp.getResponseHeader().get("params")).get("debug"));
  }


  @Test
  public void testChildDoctransformer() throws IOException, SolrServerException {
    SolrServer server = getSolrServer();
    server.deleteByQuery("*:*");
    server.commit();

    int numRootDocs = TestUtil.nextInt(random(), 10, 100);
    int maxDepth = TestUtil.nextInt(random(), 2, 5);

    Map<String,SolrInputDocument> allDocs = new HashMap<>();

    for (int i =0; i < numRootDocs; i++) {
      server.add(genNestedDocuments(allDocs, 0, maxDepth));
    }

    server.commit();

    // sanity check
    SolrQuery q = new SolrQuery("*:*");
    QueryResponse resp = server.query(q);
    assertEquals("Doc count does not match", 
                 allDocs.size(), resp.getResults().getNumFound());


    // base check - we know there is an exact number of these root docs
    q = new SolrQuery("level_i:0");
    q.setFields("*", "[child parentFilter=\"level_i:0\"]");
    resp = server.query(q);
    assertEquals("topLevel count does not match", numRootDocs,
                 resp.getResults().getNumFound());
    for (SolrDocument outDoc : resp.getResults()) {
      String docId = (String)outDoc.getFieldValue("id");
      SolrInputDocument origDoc = allDocs.get(docId);
      assertNotNull("docId not found: " + docId, origDoc);
      assertEquals("kids mismatch", 
                   origDoc.hasChildDocuments(), outDoc.hasChildDocuments());
      if (outDoc.hasChildDocuments()) {
        for (SolrDocument kid : outDoc.getChildDocuments()) {
          String kidId = (String)kid.getFieldValue("id");
          SolrInputDocument origChild = findDecendent(origDoc, kidId);
          assertNotNull(docId + " doesn't have decendent " + kidId,
                        origChild);
        }
      }
    }

    // simple check: direct verification of direct children on random docs
    {
      int parentLevel = TestUtil.nextInt(random(), 0, maxDepth);
      int kidLevel = parentLevel+1;
      String parentFilter = "level_i:" + parentLevel;
      String childFilter = "level_i:" + kidLevel;
      int maxKidCount = TestUtil.nextInt(random(), 1, 37);
      
      q = new SolrQuery("*:*");
      q.setFilterQueries(parentFilter);
      q.setFields("id,[child parentFilter=\"" + parentFilter +
                  "\" childFilter=\"" + childFilter + 
                  "\" limit=\"" + maxKidCount + "\"]");
      resp = server.query(q);
      for (SolrDocument outDoc : resp.getResults()) {
        String docId = (String)outDoc.getFieldValue("id");
        SolrInputDocument origDoc = allDocs.get(docId);
        assertNotNull("docId not found: " + docId, origDoc);
        assertEquals("kids mismatch", 
                     origDoc.hasChildDocuments(), outDoc.hasChildDocuments());
        if (outDoc.hasChildDocuments()) {
          // since we know we are looking at our direct children
          // we can verify the count
          int numOrigKids = origDoc.getChildDocuments().size();
          int numOutKids = outDoc.getChildDocuments().size();
          assertEquals("Num kids mismatch: " + numOrigKids + "/" + maxKidCount,
                       (maxKidCount < numOrigKids ? maxKidCount : numOrigKids),
                       numOutKids);
          
          for (SolrDocument kid : outDoc.getChildDocuments()) {
            String kidId = (String)kid.getFieldValue("id");
            assertEquals("kid is the wrong level",
                         kidLevel, (int)kid.getFieldValue("level_i"));
            SolrInputDocument origChild = findDecendent(origDoc, kidId);
            assertNotNull(docId + " doesn't have decendent " + kidId,
                          origChild);
          }
        }
      }
    }

    // fully randomized
    // verifications are driven only by the results
    {
      int parentLevel = TestUtil.nextInt(random(), 0, maxDepth-1);
      int kidLevelMin = TestUtil.nextInt(random(), parentLevel + 1, maxDepth);
      int kidLevelMax = TestUtil.nextInt(random(), kidLevelMin, maxDepth);

      String parentFilter = "level_i:" + parentLevel;
      String childFilter = "level_i:[" + kidLevelMin + " TO " + kidLevelMax + "]";
      int maxKidCount = TestUtil.nextInt(random(), 1, 7);
      
      q = new SolrQuery("*:*");
      if (random().nextBoolean()) {
        String name = names[TestUtil.nextInt(random(), 0, names.length-1)];
        q = new SolrQuery("name:" + name);
      }
      q.setFilterQueries(parentFilter);
      q.setFields("id,[child parentFilter=\"" + parentFilter +
                  "\" childFilter=\"" + childFilter + 
                  "\" limit=\"" + maxKidCount + "\"]");
      resp = server.query(q);
      for (SolrDocument outDoc : resp.getResults()) {
        String docId = (String)outDoc.getFieldValue("id");
        SolrInputDocument origDoc = allDocs.get(docId);
        assertNotNull("docId not found: " + docId, origDoc);
        // we can't always assert origHasKids==outHasKids, original kids
        // might not go deep enough for childFilter...
        if (outDoc.hasChildDocuments()) {
          // ...however if there are out kids, there *have* to be orig kids
          assertTrue("orig doc had no kids at all", origDoc.hasChildDocuments());
          for (SolrDocument kid : outDoc.getChildDocuments()) {
            String kidId = (String)kid.getFieldValue("id");
            int kidLevel = (int)kid.getFieldValue("level_i");
            assertTrue("kid level to high: " + kidLevelMax + "<" + kidLevel,
                       kidLevel <= kidLevelMax);
            assertTrue("kid level to low: " + kidLevelMin + ">" + kidLevel,
                       kidLevelMin <= kidLevel);
            SolrInputDocument origChild = findDecendent(origDoc, kidId);
            assertNotNull(docId + " doesn't have decendent " + kidId,
                          origChild);
          }
        }
      }
    }
  }

  @Test
  public void testFieldGlobbing() throws Exception  {
    SolrServer server = getSolrServer();

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "testFieldGlobbing");
    doc.addField("x_s", "x");
    doc.addField("y_s", "y");
    doc.addField("z_s", "z");
    server.add(doc);
    server.commit();

    // id and glob
    QueryResponse response = server.query(new SolrQuery("id:testFieldGlobbing").addField("id").addField("*_s"));
    assertEquals("Document not found", 1, response.getResults().getNumFound());
    assertEquals("All requested fields were not returned", 4, response.getResults().get(0).getFieldNames().size());

    // just globs
    response = server.query(new SolrQuery("id:testFieldGlobbing").addField("*_s"));
    assertEquals("Document not found", 1, response.getResults().getNumFound());
    assertEquals("All requested fields were not returned", 3, response.getResults().get(0).getFieldNames().size());

    // just id
    response = server.query(new SolrQuery("id:testFieldGlobbing").addField("id"));
    assertEquals("Document not found", 1, response.getResults().getNumFound());
    assertEquals("All requested fields were not returned", 1, response.getResults().get(0).getFieldNames().size());

    // id and pseudo field and glob
    response = server.query(new SolrQuery("id:testFieldGlobbing").addField("id").addField("[docid]").addField("*_s"));
    assertEquals("Document not found", 1, response.getResults().getNumFound());
    assertEquals("All requested fields were not returned", 5, response.getResults().get(0).getFieldNames().size());

    // pseudo field and glob
    response = server.query(new SolrQuery("id:testFieldGlobbing").addField("[docid]").addField("*_s"));
    assertEquals("Document not found", 1, response.getResults().getNumFound());
    assertEquals("All requested fields were not returned", 4, response.getResults().get(0).getFieldNames().size());

    // just a pseudo field
    response = server.query(new SolrQuery("id:testFieldGlobbing").addField("[docid]"));
    assertEquals("Document not found", 1, response.getResults().getNumFound());
    assertEquals("All requested fields were not returned", 1, response.getResults().get(0).getFieldNames().size());

    // only score
    response = server.query(new SolrQuery("id:testFieldGlobbing").addField("score"));
    assertEquals("Document not found", 1, response.getResults().getNumFound());
    assertEquals("All requested fields were not returned", 1, response.getResults().get(0).getFieldNames().size());

    // pseudo field and score
    response = server.query(new SolrQuery("id:testFieldGlobbing").addField("score").addField("[docid]"));
    assertEquals("Document not found", 1, response.getResults().getNumFound());
    assertEquals("All requested fields were not returned", 2, response.getResults().get(0).getFieldNames().size());

    // score and globs
    response = server.query(new SolrQuery("id:testFieldGlobbing").addField("score").addField("*_s"));
    assertEquals("Document not found", 1, response.getResults().getNumFound());
    assertEquals("All requested fields were not returned", 4, response.getResults().get(0).getFieldNames().size());
  }

  /** 
   * Depth first search of a SolrInputDocument looking for a decendent by id, 
   * returns null if it's not a decendent 
   */
  private SolrInputDocument findDecendent(SolrInputDocument parent, String childId) {
    if (childId.equals(parent.getFieldValue("id"))) {
      return parent;
    }
    if (! parent.hasChildDocuments() ) {
      return null;
    }
    for (SolrInputDocument kid : parent.getChildDocuments()) {
      SolrInputDocument result = findDecendent(kid, childId);
      if (null != result) {
        return result;
      }
    }
    return null;
  }

  /** used by genNestedDocuments */
  private int idCounter = 0;
  /** used by genNestedDocuments */
  private static final String[] names 
    = new String[] { "java","python","scala","ruby","clojure" };

  /**
   * recursive method for generating a document, which may also have child documents;
   * adds all documents constructed (including decendents) to allDocs via their id 
   */
  private SolrInputDocument genNestedDocuments(Map<String,SolrInputDocument> allDocs, 
                                               int thisLevel,
                                               int maxDepth) {
    String id = "" + (idCounter++);
    SolrInputDocument sdoc = new SolrInputDocument();
    allDocs.put(id, sdoc);

    sdoc.addField("id", id);
    sdoc.addField("level_i", thisLevel);
    sdoc.addField("name", names[TestUtil.nextInt(random(), 0, names.length-1)]);
    
    if (0 < maxDepth) {
      // NOTE: range include negative to increase odds of no kids
      int numKids = TestUtil.nextInt(random(), -2, 7);
      for(int i=0; i<numKids; i++) {
        sdoc.addChildDocument(genNestedDocuments(allDocs, thisLevel+1, maxDepth-1));
      }
    }
    return sdoc;
  }
}
