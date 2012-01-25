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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.handler.XmlUpdateRequestHandler;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.util.RefCounted;

class NewSearcherListener implements SolrEventListener {

  enum TriggerOn {Both, Soft, Hard}

  private volatile boolean triggered = false;
  private volatile TriggerOn lastType;
  private volatile TriggerOn triggerOnType;
  private volatile SolrIndexSearcher newSearcher;

  public NewSearcherListener() {
    this(TriggerOn.Both);
  }

  public NewSearcherListener(TriggerOn type) {
    this.triggerOnType = type;
  }

  @Override
  public void init(NamedList args) {}

  @Override
  public void newSearcher(SolrIndexSearcher newSearcher,
      SolrIndexSearcher currentSearcher) {
    if (triggerOnType == TriggerOn.Soft && lastType == TriggerOn.Soft) {
      triggered = true;
    } else if (triggerOnType == TriggerOn.Hard && lastType == TriggerOn.Hard) {
      triggered = true;
    } else if (triggerOnType == TriggerOn.Both) {
      triggered = true;
    }
    this.newSearcher = newSearcher;
    // log.info("TEST: newSearcher event: triggered="+triggered+" newSearcher="+newSearcher);
  }

  @Override
  public void postCommit() {
    lastType = TriggerOn.Hard;
  }

  @Override
  public void postSoftCommit() {
    lastType = TriggerOn.Soft;
  }

  public void reset() {
    triggered = false;
    // log.info("TEST: trigger reset");
  }

  boolean waitForNewSearcher(int timeout) {
    long timeoutTime = System.currentTimeMillis() + timeout;
    while (System.currentTimeMillis() < timeoutTime) {
      if (triggered) {
        // check if the new searcher has been registered yet
        RefCounted<SolrIndexSearcher> registeredSearcherH = newSearcher.getCore().getSearcher();
        SolrIndexSearcher registeredSearcher = registeredSearcherH.get();
        registeredSearcherH.decref();
        if (registeredSearcher == newSearcher) return true;
        // log.info("TEST: waiting for searcher " + newSearcher + " to be registered.  current=" + registeredSearcher);
      }

      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {}

    }
    return false;
  }
}

public class AutoCommitTest extends AbstractSolrTestCase {

  @Override
  public String getSchemaFile() { return "schema.xml"; }
  @Override
  public String getSolrConfigFile() { return "solrconfig.xml"; }

  public static void verbose(Object... args) {
    if (!VERBOSE) return;
    StringBuilder sb = new StringBuilder("###TEST:");
    sb.append(Thread.currentThread().getName());
    sb.append(':');
    for (Object o : args) {
      sb.append(' ');
      sb.append(o.toString());
    }
    log.info(sb.toString());
    // System.out.println(sb.toString());
  }

  /**
   * Take a string and make it an iterable ContentStream
   * 
   * This should be moved to a helper class. (it is useful for the client too!)
   */
  public static Collection<ContentStream> toContentStreams( final String str, final String contentType )
  {
    ArrayList<ContentStream> streams = new ArrayList<ContentStream>();
    ContentStreamBase stream = new ContentStreamBase.StringStream( str );
    stream.setContentType( contentType );
    streams.add( stream );
    return streams;
  }

  public void testMaxDocs() throws Exception {

    SolrCore core = h.getCore();
    NewSearcherListener trigger = new NewSearcherListener();

    DirectUpdateHandler2 updateHandler = (DirectUpdateHandler2)core.getUpdateHandler();
    CommitTracker tracker = updateHandler.commitTracker;
    tracker.setTimeUpperBound(-1);
    tracker.setDocsUpperBound(14);
    core.registerNewSearcherListener(trigger);
  
    
    XmlUpdateRequestHandler handler = new XmlUpdateRequestHandler();
    handler.init( null );
    
    MapSolrParams params = new MapSolrParams( new HashMap<String, String>() );
    
    // Add documents
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequestBase req = new SolrQueryRequestBase( core, params ) {};
    for( int i=0; i<14; i++ ) {
      req.setContentStreams( toContentStreams(
        adoc("id", Integer.toString(i), "subject", "info" ), null ) );
      handler.handleRequest( req, rsp );
    }
    // It should not be there right away
    assertQ("shouldn't find any", req("id:1") ,"//result[@numFound=0]" );
    assertEquals( 0, tracker.getCommitCount());

    req.setContentStreams( toContentStreams(
        adoc("id", "14", "subject", "info" ), null ) );
    handler.handleRequest( req, rsp );

    assertTrue(trigger.waitForNewSearcher(15000));

    req.setContentStreams( toContentStreams(
        adoc("id", "15", "subject", "info" ), null ) );
    handler.handleRequest( req, rsp );
      
    // Now make sure we can find it
    assertQ("should find one", req("id:14") ,"//result[@numFound=1]" );
    assertEquals( 1, tracker.getCommitCount());
    // But not the one added afterward
    assertQ("should not find one", req("id:15") ,"//result[@numFound=0]" );
    assertEquals( 1, tracker.getCommitCount());
    
  }

  public void testMaxTime() throws Exception {
    SolrCore core = h.getCore();
    NewSearcherListener trigger = new NewSearcherListener();    
    core.registerNewSearcherListener(trigger);
    DirectUpdateHandler2 updater = (DirectUpdateHandler2) core.getUpdateHandler();
    CommitTracker tracker = updater.commitTracker;
    // too low of a number can cause a slow host to commit before the test code checks that it
    // isn't there... causing a failure at "shouldn't find any"
    tracker.setTimeUpperBound(1000);
    tracker.setDocsUpperBound(-1);
    // updater.commitCallbacks.add(trigger);
    
    XmlUpdateRequestHandler handler = new XmlUpdateRequestHandler();
    handler.init( null );
    
    MapSolrParams params = new MapSolrParams( new HashMap<String, String>() );
    
    // Add a single document
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequestBase req = new SolrQueryRequestBase( core, params ) {};
    req.setContentStreams( toContentStreams(
      adoc("id", "529", "field_t", "what's inside?", "subject", "info"), null ) );
    trigger.reset();
    handler.handleRequest( req, rsp );

    // Check it it is in the index
    assertQ("shouldn't find any", req("id:529") ,"//result[@numFound=0]" );

    // Wait longer than the autocommit time
    assertTrue(trigger.waitForNewSearcher(45000));
    trigger.reset();
    req.setContentStreams( toContentStreams(
      adoc("id", "530", "field_t", "what's inside?", "subject", "info"), null ) );
    handler.handleRequest( req, rsp );
      
    // Now make sure we can find it
    assertQ("should find one", req("id:529") ,"//result[@numFound=1]" );
    // But not this one
    assertQ("should find none", req("id:530") ,"//result[@numFound=0]" );
    
    // Delete the document
    assertU( delI("529") );
    assertQ("deleted, but should still be there", req("id:529") ,"//result[@numFound=1]" );
    // Wait longer than the autocommit time
    assertTrue(trigger.waitForNewSearcher(30000));
    trigger.reset();
    req.setContentStreams( toContentStreams(
      adoc("id", "550", "field_t", "what's inside?", "subject", "info"), null ) );
    handler.handleRequest( req, rsp );
    assertEquals( 2, tracker.getCommitCount() );
    assertQ("deleted and time has passed", req("id:529") ,"//result[@numFound=0]" );
    
    // now make the call 10 times really fast and make sure it 
    // only commits once
    req.setContentStreams( toContentStreams(
        adoc("id", "500" ), null ) );
    for( int i=0;i<10; i++ ) {
      handler.handleRequest( req, rsp );
    }
    assertQ("should not be there yet", req("id:500") ,"//result[@numFound=0]" );
    
    // Wait longer than the autocommit time
    assertTrue(trigger.waitForNewSearcher(45000));
    trigger.reset();
    
    req.setContentStreams( toContentStreams(
      adoc("id", "531", "field_t", "what's inside?", "subject", "info"), null ) );
    handler.handleRequest( req, rsp );
    assertEquals( 3, tracker.getCommitCount() );

    assertQ("now it should", req("id:500") ,"//result[@numFound=1]" );
  }
  
  public void testCommitWithin() throws Exception {
    SolrCore core = h.getCore();
    NewSearcherListener trigger = new NewSearcherListener();    
    core.registerNewSearcherListener(trigger);
    DirectUpdateHandler2 updater = (DirectUpdateHandler2) core.getUpdateHandler();
    CommitTracker tracker = updater.commitTracker;
    tracker.setTimeUpperBound(0);
    tracker.setDocsUpperBound(-1);
    
    XmlUpdateRequestHandler handler = new XmlUpdateRequestHandler();
    handler.init( null );
    
    MapSolrParams params = new MapSolrParams( new HashMap<String, String>() );
    
    // Add a single document with commitWithin == 1 second
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequestBase req = new SolrQueryRequestBase( core, params ) {};
    req.setContentStreams( toContentStreams(
      adoc(1000, "id", "529", "field_t", "what's inside?", "subject", "info"), null ) );
    trigger.reset();
    handler.handleRequest( req, rsp );

    // Check it isn't in the index
    assertQ("shouldn't find any", req("id:529") ,"//result[@numFound=0]" );
    
    // Wait longer than the commitWithin time
    assertTrue("commitWithin failed to commit", trigger.waitForNewSearcher(30000));

    // Add one document without commitWithin
    req.setContentStreams( toContentStreams(
        adoc("id", "530", "field_t", "what's inside?", "subject", "info"), null ) );
      trigger.reset();
      handler.handleRequest( req, rsp );
      
    // Check it isn't in the index
    assertQ("shouldn't find any", req("id:530") ,"//result[@numFound=0]" );
    
    // Delete one document with commitWithin
    req.setContentStreams( toContentStreams(
      delI("529", "commitWithin", "1000"), null ) );
    trigger.reset();
    handler.handleRequest( req, rsp );
      
    // Now make sure we can find it
    assertQ("should find one", req("id:529") ,"//result[@numFound=1]" );
    
    // Wait for the commit to happen
    assertTrue("commitWithin failed to commit", trigger.waitForNewSearcher(30000));
    
    // Now we shouldn't find it
    assertQ("should find none", req("id:529") ,"//result[@numFound=0]" );
    // ... but we should find the new one
    assertQ("should find one", req("id:530") ,"//result[@numFound=1]" );
    
    trigger.reset();
    
    // now make the call 10 times really fast and make sure it 
    // only commits once
    req.setContentStreams( toContentStreams(
        adoc(1000, "id", "500" ), null ) );
    for( int i=0;i<10; i++ ) {
      handler.handleRequest( req, rsp );
    }
    assertQ("should not be there yet", req("id:500") ,"//result[@numFound=0]" );
    
    // the same for the delete
    req.setContentStreams( toContentStreams(
        delI("530", "commitWithin", "1000"), null ) );
    for( int i=0;i<10; i++ ) {
      handler.handleRequest( req, rsp );
    }
    assertQ("should be there", req("id:530") ,"//result[@numFound=1]" );
    
    assertTrue("commitWithin failed to commit", trigger.waitForNewSearcher(30000));
    assertQ("should be there", req("id:500") ,"//result[@numFound=1]" );
    assertQ("should not be there", req("id:530") ,"//result[@numFound=0]" );
    
    assertEquals(3, tracker.getCommitCount());
  }

}
