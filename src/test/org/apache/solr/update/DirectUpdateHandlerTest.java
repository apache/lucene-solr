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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexReader;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.util.RefCounted;

/**
 * 
 *
 */
public class DirectUpdateHandlerTest extends AbstractSolrTestCase {

  public String getSchemaFile() { return "schema12.xml"; }
  public String getSolrConfigFile() { return "solrconfig.xml"; }
  

  public void testRequireUniqueKey() throws Exception 
  {
    SolrCore core = h.getCore();
    
    UpdateHandler updater = core.getUpdateHandler();
    
    AddUpdateCommand cmd = new AddUpdateCommand();
    cmd.overwriteCommitted = true;
    cmd.overwritePending = true;
    cmd.allowDups = false;
    
    // Add a valid document
    cmd.doc = new Document();
    cmd.doc.add( new Field( "id", "AAA", Store.YES, Index.UN_TOKENIZED ) );
    cmd.doc.add( new Field( "subject", "xxxxx", Store.YES, Index.UN_TOKENIZED ) );
    updater.addDoc( cmd );
    
    // Add a document with multiple ids
    cmd.indexedId = null;  // reset the id for this add
    cmd.doc = new Document();
    cmd.doc.add( new Field( "id", "AAA", Store.YES, Index.UN_TOKENIZED ) );
    cmd.doc.add( new Field( "id", "BBB", Store.YES, Index.UN_TOKENIZED ) );
    cmd.doc.add( new Field( "subject", "xxxxx", Store.YES, Index.UN_TOKENIZED ) );
    try {
      updater.addDoc( cmd );
      fail( "added a document with multiple ids" );
    }
    catch( SolrException ex ) { } // expected

    // Add a document without an id
    cmd.indexedId = null;  // reset the id for this add
    cmd.doc = new Document();
    cmd.doc.add( new Field( "subject", "xxxxx", Store.YES, Index.UN_TOKENIZED ) );
    try {
      updater.addDoc( cmd );
      fail( "added a document without an ids" );
    }
    catch( SolrException ex ) { } // expected
  }

  public void testUncommit() throws Exception {
    addSimpleDoc("A");

    // search - not committed - "A" should not be found.
    Map<String,String> args = new HashMap<String, String>();
    args.put( CommonParams.Q, "id:A" );
    args.put( "indent", "true" );
    SolrQueryRequest req = new LocalSolrQueryRequest( h.getCore(), new MapSolrParams( args) );
    assertQ("\"A\" should not be found.", req
            ,"//*[@numFound='0']"
            );
  }

  public void testAddCommit() throws Exception {
    addSimpleDoc("A");

    // commit "A"
    SolrCore core = h.getCore();
    UpdateHandler updater = core.getUpdateHandler();
    CommitUpdateCommand cmtCmd = new CommitUpdateCommand(false);
    cmtCmd.waitSearcher = true;
    updater.commit(cmtCmd);

    // search - "A" should be found.
    Map<String,String> args = new HashMap<String, String>();
    args.put( CommonParams.Q, "id:A" );
    args.put( "indent", "true" );
    SolrQueryRequest req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
    assertQ("\"A\" should be found.", req
            ,"//*[@numFound='1']"
            ,"//result/doc[1]/str[@name='id'][.='A']"
            );
  }

  public void testDeleteCommit() throws Exception {
    addSimpleDoc("A");
    addSimpleDoc("B");

    // commit "A", "B"
    SolrCore core = h.getCore();
    UpdateHandler updater = core.getUpdateHandler();
    CommitUpdateCommand cmtCmd = new CommitUpdateCommand(false);
    cmtCmd.waitSearcher = true;
    updater.commit(cmtCmd);

    // search - "A","B" should be found.
    Map<String,String> args = new HashMap<String, String>();
    args.put( CommonParams.Q, "id:A OR id:B" );
    args.put( "indent", "true" );
    SolrQueryRequest req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
    assertQ("\"A\" and \"B\" should be found.", req
            ,"//*[@numFound='2']"
            ,"//result/doc[1]/str[@name='id'][.='A']"
            ,"//result/doc[2]/str[@name='id'][.='B']"
            );

    // delete "B"
    deleteSimpleDoc("B");

    // search - "A","B" should be found.
    assertQ("\"A\" and \"B\" should be found.", req
            ,"//*[@numFound='2']"
            ,"//result/doc[1]/str[@name='id'][.='A']"
            ,"//result/doc[2]/str[@name='id'][.='B']"
            );
 
    // commit
    updater.commit(cmtCmd);
    
    // search - "B" should not be found.
    assertQ("\"B\" should not be found.", req
        ,"//*[@numFound='1']"
        ,"//result/doc[1]/str[@name='id'][.='A']"
        );
  }

  public void testAddRollback() throws Exception {
    addSimpleDoc("A");

    // commit "A"
    SolrCore core = h.getCore();
    UpdateHandler updater = core.getUpdateHandler();
    assertTrue( updater instanceof DirectUpdateHandler2 );
    DirectUpdateHandler2 duh2 = (DirectUpdateHandler2)updater;
    CommitUpdateCommand cmtCmd = new CommitUpdateCommand(false);
    cmtCmd.waitSearcher = true;
    assertEquals( 1, duh2.addCommands.get() );
    assertEquals( 1, duh2.addCommandsCumulative.get() );
    assertEquals( 0, duh2.commitCommands.get() );
    updater.commit(cmtCmd);
    assertEquals( 0, duh2.addCommands.get() );
    assertEquals( 1, duh2.addCommandsCumulative.get() );
    assertEquals( 1, duh2.commitCommands.get() );

    addSimpleDoc("B");

    // rollback "B"
    RollbackUpdateCommand rbkCmd = new RollbackUpdateCommand();
    assertEquals( 1, duh2.addCommands.get() );
    assertEquals( 2, duh2.addCommandsCumulative.get() );
    assertEquals( 0, duh2.rollbackCommands.get() );
    updater.rollback(rbkCmd);
    assertEquals( 0, duh2.addCommands.get() );
    assertEquals( 1, duh2.addCommandsCumulative.get() );
    assertEquals( 1, duh2.rollbackCommands.get() );
    
    // search - "B" should not be found.
    Map<String,String> args = new HashMap<String, String>();
    args.put( CommonParams.Q, "id:A OR id:B" );
    args.put( "indent", "true" );
    SolrQueryRequest req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
    assertQ("\"B\" should not be found.", req
            ,"//*[@numFound='1']"
            ,"//result/doc[1]/str[@name='id'][.='A']"
            );

    // Add a doc after the rollback to make sure we can continue to add/delete documents
    // after a rollback as normal
    addSimpleDoc("ZZZ");
    assertU(commit());
    assertQ("\"ZZZ\" must be found.", req("q", "id:ZZZ")
            ,"//*[@numFound='1']"
            ,"//result/doc[1]/str[@name='id'][.='ZZZ']"
            );
  }

  public void testDeleteRollback() throws Exception {
    addSimpleDoc("A");
    addSimpleDoc("B");

    // commit "A", "B"
    SolrCore core = h.getCore();
    UpdateHandler updater = core.getUpdateHandler();
    assertTrue( updater instanceof DirectUpdateHandler2 );
    DirectUpdateHandler2 duh2 = (DirectUpdateHandler2)updater;
    CommitUpdateCommand cmtCmd = new CommitUpdateCommand(false);
    cmtCmd.waitSearcher = true;
    assertEquals( 2, duh2.addCommands.get() );
    assertEquals( 2, duh2.addCommandsCumulative.get() );
    assertEquals( 0, duh2.commitCommands.get() );
    updater.commit(cmtCmd);
    assertEquals( 0, duh2.addCommands.get() );
    assertEquals( 2, duh2.addCommandsCumulative.get() );
    assertEquals( 1, duh2.commitCommands.get() );

    // search - "A","B" should be found.
    Map<String,String> args = new HashMap<String, String>();
    args.put( CommonParams.Q, "id:A OR id:B" );
    args.put( "indent", "true" );
    SolrQueryRequest req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
    assertQ("\"A\" and \"B\" should be found.", req
            ,"//*[@numFound='2']"
            ,"//result/doc[1]/str[@name='id'][.='A']"
            ,"//result/doc[2]/str[@name='id'][.='B']"
            );

    // delete "B"
    deleteSimpleDoc("B");
    
    // search - "A","B" should be found.
    assertQ("\"A\" and \"B\" should be found.", req
        ,"//*[@numFound='2']"
        ,"//result/doc[1]/str[@name='id'][.='A']"
        ,"//result/doc[2]/str[@name='id'][.='B']"
        );

    // rollback "B"
    RollbackUpdateCommand rbkCmd = new RollbackUpdateCommand();
    assertEquals( 1, duh2.deleteByIdCommands.get() );
    assertEquals( 1, duh2.deleteByIdCommandsCumulative.get() );
    assertEquals( 0, duh2.rollbackCommands.get() );
    updater.rollback(rbkCmd);
    assertEquals( 0, duh2.deleteByIdCommands.get() );
    assertEquals( 0, duh2.deleteByIdCommandsCumulative.get() );
    assertEquals( 1, duh2.rollbackCommands.get() );
    
    // search - "B" should be found.
    assertQ("\"B\" should be found.", req
        ,"//*[@numFound='2']"
        ,"//result/doc[1]/str[@name='id'][.='A']"
        ,"//result/doc[2]/str[@name='id'][.='B']"
        );

    // Add a doc after the rollback to make sure we can continue to add/delete documents
    // after a rollback as normal
    addSimpleDoc("ZZZ");
    assertU(commit());
    assertQ("\"ZZZ\" must be found.", req("q", "id:ZZZ")
            ,"//*[@numFound='1']"
            ,"//result/doc[1]/str[@name='id'][.='ZZZ']"
            );
  }

  public void testExpungeDeletes() throws Exception {
    assertU(adoc("id","1"));
    assertU(adoc("id","2"));
    assertU(commit());

    assertU(adoc("id","3"));
    assertU(adoc("id","2"));
    assertU(adoc("id","4"));
    assertU(commit());

    SolrQueryRequest sr = req("q","foo");
    SolrIndexReader r = sr.getSearcher().getReader();
    assertTrue(r.maxDoc() > r.numDocs());   // should have deletions
    assertTrue(r.getLeafReaders().length > 1);  // more than 1 segment
    sr.close();

    assertU(commit("expungeDeletes","true"));

    sr = req("q","foo");
    r = sr.getSearcher().getReader();
    assertEquals(r.maxDoc(), r.numDocs());  // no deletions
    assertEquals(4,r.maxDoc());             // no dups
    assertTrue(r.getLeafReaders().length > 1);  // still more than 1 segment
    sr.close();
  }
  
  private void addSimpleDoc(String id) throws Exception {
    SolrCore core = h.getCore();
    
    UpdateHandler updater = core.getUpdateHandler();
    
    AddUpdateCommand cmd = new AddUpdateCommand();
    cmd.overwriteCommitted = true;
    cmd.overwritePending = true;
    cmd.allowDups = false;
    
    // Add a document
    cmd.doc = new Document();
    cmd.doc.add( new Field( "id", id, Store.YES, Index.UN_TOKENIZED ) );
    updater.addDoc( cmd );
  }
  
  private void deleteSimpleDoc(String id) throws Exception {
    SolrCore core = h.getCore();
    
    UpdateHandler updater = core.getUpdateHandler();
    
    // Delete the document
    DeleteUpdateCommand cmd = new DeleteUpdateCommand();
    cmd.id = id;
    cmd.fromCommitted = true;
    cmd.fromPending = true;
    
    updater.delete(cmd);
  }
}
