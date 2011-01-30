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
import org.apache.solr.core.*;
import org.apache.solr.search.*;
import org.apache.solr.handler.XmlUpdateRequestHandler;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.util.RefCounted;

class CommitListener implements SolrEventListener {
  public volatile boolean triggered = false;
  public volatile SolrIndexSearcher currentSearcher;
  public SolrCore core;

  public CommitListener(SolrCore core) {
    this.core = core;
  }

  public void init(NamedList args) {}

  public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
    this.currentSearcher = currentSearcher;
    triggered = true;
  }
  public void postCommit() {
    triggered = true;
  }
  public void reset() {
    triggered=false;
  }

  public boolean waitForCommit(int timeout) {
    //triggered = false;
    
    for (int towait=timeout; towait > 0; towait -= 250) {
      try {
        if (triggered) {
          RefCounted<SolrIndexSearcher> holder = core.getSearcher();
          SolrIndexSearcher s = holder.get();
          holder.decref();
          // since there could be two commits in a row, don't test for a specific new searcher
          // just test that the old one has been replaced.
          if (s != currentSearcher) return true;
          // it may be that a commit just happened, but the new searcher hasn't been registered yet.
        }
        Thread.sleep( 250 );
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

  /* This test is causing too many failures on one of the build slaves.
     Temporarily disabled. -Mike Klaas */
  public void XXXtestMaxDocs() throws Exception {

    SolrCore core = h.getCore();
    CommitListener trigger = new CommitListener(core);

    DirectUpdateHandler2 updater = (DirectUpdateHandler2)core.getUpdateHandler();
    DirectUpdateHandler2.CommitTracker tracker = updater.tracker;
    tracker.timeUpperBound = 100000;
    tracker.docsUpperBound = 14;
    // updater.commitCallbacks.add(trigger);
    
    XmlUpdateRequestHandler handler = new XmlUpdateRequestHandler();
    handler.init( null );
    
    MapSolrParams params = new MapSolrParams( new HashMap<String, String>() );
    
    // Add a single document
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequestBase req = new SolrQueryRequestBase( core, params ) {};
    for( int i=0; i<14; i++ ) {
      req.setContentStreams( toContentStreams(
        adoc("id", "A"+i, "subject", "info" ), null ) );
      handler.handleRequest( req, rsp );
    }
    // It should not be there right away
    assertQ("shouldn't find any", req("id:A1") ,"//result[@numFound=0]" );
    assertEquals( 0, tracker.getCommitCount());

    req.setContentStreams( toContentStreams(
        adoc("id", "A14", "subject", "info" ), null ) );
    handler.handleRequest( req, rsp );
    // Wait longer than the autocommit time
    assertTrue(trigger.waitForCommit(20000));

    req.setContentStreams( toContentStreams(
        adoc("id", "A15", "subject", "info" ), null ) );
    handler.handleRequest( req, rsp );
      
    // Now make sure we can find it
    assertQ("should find one", req("id:A14") ,"//result[@numFound=1]" );
    assertEquals( 1, tracker.getCommitCount());
    // But not the one added afterward
    assertQ("should not find one", req("id:A15") ,"//result[@numFound=0]" );
    assertEquals( 1, tracker.getCommitCount());
    
  }

  public void testMaxTime() throws Exception {
    SolrCore core = h.getCore();
    CommitListener trigger = new CommitListener(core);    
    core.registerNewSearcherListener(trigger);
    DirectUpdateHandler2 updater = (DirectUpdateHandler2) core.getUpdateHandler();
    DirectUpdateHandler2.CommitTracker tracker = updater.tracker;
    // too low of a number can cause a slow host to commit before the test code checks that it
    // isn't there... causing a failure at "shouldn't find any"
    tracker.timeUpperBound = 1000;
    tracker.docsUpperBound = -1;
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
    assertTrue(trigger.waitForCommit(30000));
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
    assertTrue(trigger.waitForCommit(30000));
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
    assertTrue(trigger.waitForCommit(30000));
    trigger.reset();
    
    req.setContentStreams( toContentStreams(
      adoc("id", "531", "field_t", "what's inside?", "subject", "info"), null ) );
    handler.handleRequest( req, rsp );
    assertEquals( 3, tracker.getCommitCount() );

    assertQ("now it should", req("id:500") ,"//result[@numFound=1]" );
    assertQ("but not this", req("id:531") ,"//result[@numFound=0]" );
  }

}
