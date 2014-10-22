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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 *
 */
public class DirectUpdateHandlerTest extends SolrTestCaseJ4 {

  static String savedFactory;
  @BeforeClass
  public static void beforeClass() throws Exception {
    savedFactory = System.getProperty("solr.DirectoryFactory");
    System.setProperty("solr.directoryFactory", "org.apache.solr.core.MockFSDirectoryFactory");
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    System.setProperty("solr.tests.mergePolicy", TieredMergePolicy.class.getName());
    initCore("solrconfig.xml", "schema12.xml");
  }
  
  @AfterClass
  public static void afterClass() {
    if (savedFactory == null) {
      System.clearProperty("solr.directoryFactory");
    } else {
      System.setProperty("solr.directoryFactory", savedFactory);
    }
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
  }

  @Test
  public void testRequireUniqueKey() throws Exception {
    // Add a valid document
    assertU(adoc("id","1"));

    // More than one id should fail
    assertFailedU(adoc("id","2", "id","ignore_exception", "text","foo"));

    // No id should fail
    ignoreException("id");
    assertFailedU(adoc("text","foo"));
    resetExceptionIgnores();
  }



  @Test
  public void testBasics() throws Exception {
    
    assertNull("This test requires a schema that has no version field, " +
               "it appears the schema file in use has been edited to violate " +
               "this requirement",
               h.getCore().getLatestSchema().getFieldOrNull(VersionInfo.VERSION_FIELD));

    assertU(adoc("id","5"));
    assertU(adoc("id","6"));

    // search - not committed - docs should not be found.
    assertQ(req("q","id:5"), "//*[@numFound='0']");
    assertQ(req("q","id:6"), "//*[@numFound='0']");

    assertU(commit());

    // now they should be there
    assertQ(req("q","id:5"), "//*[@numFound='1']");
    assertQ(req("q","id:6"), "//*[@numFound='1']");

    // now delete one
    assertU(delI("5"));

    // not committed yet
    assertQ(req("q","id:5"), "//*[@numFound='1']");

    assertU(commit());
    
    // 5 should be gone
    assertQ(req("q","id:5"), "//*[@numFound='0']");
    assertQ(req("q","id:6"), "//*[@numFound='1']");

    // now delete all
    assertU(delQ("*:*"));

    // not committed yet
    assertQ(req("q","id:6"), "//*[@numFound='1']");

    assertU(commit());

    // 6 should be gone
    assertQ(req("q","id:6"), "//*[@numFound='0']");

  }


  @Test
  public void testAddRollback() throws Exception {
    // re-init the core
    deleteCore();
    initCore("solrconfig.xml", "schema12.xml");

    assertU(adoc("id","A"));

    // commit "A"
    SolrCore core = h.getCore();
    UpdateHandler updater = core.getUpdateHandler();
    assertTrue( updater instanceof DirectUpdateHandler2 );
    DirectUpdateHandler2 duh2 = (DirectUpdateHandler2)updater;
    SolrQueryRequest ureq = req();
    CommitUpdateCommand cmtCmd = new CommitUpdateCommand(ureq, false);
    cmtCmd.waitSearcher = true;
    assertEquals( 1, duh2.addCommands.get() );
    assertEquals( 1, duh2.addCommandsCumulative.get() );
    assertEquals( 0, duh2.commitCommands.get() );
    updater.commit(cmtCmd);
    assertEquals( 0, duh2.addCommands.get() );
    assertEquals( 1, duh2.addCommandsCumulative.get() );
    assertEquals( 1, duh2.commitCommands.get() );
    ureq.close();

    assertU(adoc("id","B"));

    // rollback "B"
    ureq = req();
    RollbackUpdateCommand rbkCmd = new RollbackUpdateCommand(ureq);
    assertEquals( 1, duh2.addCommands.get() );
    assertEquals( 2, duh2.addCommandsCumulative.get() );
    assertEquals( 0, duh2.rollbackCommands.get() );
    updater.rollback(rbkCmd);
    assertEquals( 0, duh2.addCommands.get() );
    assertEquals( 1, duh2.addCommandsCumulative.get() );
    assertEquals( 1, duh2.rollbackCommands.get() );
    ureq.close();
    
    // search - "B" should not be found.
    Map<String,String> args = new HashMap<>();
    args.put( CommonParams.Q, "id:A OR id:B" );
    args.put( "indent", "true" );
    SolrQueryRequest req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
    assertQ("\"B\" should not be found.", req
            ,"//*[@numFound='1']"
            ,"//result/doc[1]/str[@name='id'][.='A']"
            );

    // Add a doc after the rollback to make sure we can continue to add/delete documents
    // after a rollback as normal
    assertU(adoc("id","ZZZ"));
    assertU(commit());
    assertQ("\"ZZZ\" must be found.", req("q", "id:ZZZ")
            ,"//*[@numFound='1']"
            ,"//result/doc[1]/str[@name='id'][.='ZZZ']"
            );
  }

  @Test
  public void testDeleteRollback() throws Exception {
    // re-init the core
    deleteCore();
    initCore("solrconfig.xml", "schema12.xml");

    assertU(adoc("id","A"));
    assertU(adoc("id","B"));

    // commit "A", "B"
    SolrCore core = h.getCore();
    UpdateHandler updater = core.getUpdateHandler();
    assertTrue( updater instanceof DirectUpdateHandler2 );
    DirectUpdateHandler2 duh2 = (DirectUpdateHandler2)updater;
    SolrQueryRequest ureq = req();
    CommitUpdateCommand cmtCmd = new CommitUpdateCommand(ureq, false);
    cmtCmd.waitSearcher = true;
    assertEquals( 2, duh2.addCommands.get() );
    assertEquals( 2, duh2.addCommandsCumulative.get() );
    assertEquals( 0, duh2.commitCommands.get() );
    updater.commit(cmtCmd);
    assertEquals( 0, duh2.addCommands.get() );
    assertEquals( 2, duh2.addCommandsCumulative.get() );
    assertEquals( 1, duh2.commitCommands.get() );
    ureq.close();

    // search - "A","B" should be found.
    Map<String,String> args = new HashMap<>();
    args.put( CommonParams.Q, "id:A OR id:B" );
    args.put( "indent", "true" );
    SolrQueryRequest req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
    assertQ("\"A\" and \"B\" should be found.", req
            ,"//*[@numFound='2']"
            ,"//result/doc[1]/str[@name='id'][.='A']"
            ,"//result/doc[2]/str[@name='id'][.='B']"
            );

    // delete "B"
    assertU(delI("B"));

    // search - "A","B" should be found.
    assertQ("\"A\" and \"B\" should be found.", req
        ,"//*[@numFound='2']"
        ,"//result/doc[1]/str[@name='id'][.='A']"
        ,"//result/doc[2]/str[@name='id'][.='B']"
        );

    // rollback "B"
    ureq = req();
    RollbackUpdateCommand rbkCmd = new RollbackUpdateCommand(ureq);
    assertEquals( 1, duh2.deleteByIdCommands.get() );
    assertEquals( 1, duh2.deleteByIdCommandsCumulative.get() );
    assertEquals( 0, duh2.rollbackCommands.get() );
    updater.rollback(rbkCmd);
    ureq.close();
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
    assertU(adoc("id","ZZZ"));
    assertU(commit());
    assertQ("\"ZZZ\" must be found.", req("q", "id:ZZZ")
            ,"//*[@numFound='1']"
            ,"//result/doc[1]/str[@name='id'][.='ZZZ']"
            );
  }

  @Test
  public void testExpungeDeletes() throws Exception {
    assertU(adoc("id","1"));
    assertU(adoc("id","2"));
    assertU(commit());

    assertU(adoc("id","3"));
    assertU(adoc("id","2")); // dup, triggers delete
    assertU(adoc("id","4"));
    assertU(commit());

    SolrQueryRequest sr = req("q","foo");
    DirectoryReader r = sr.getSearcher().getIndexReader();
    assertTrue("maxDoc !> numDocs ... expected some deletions",
               r.maxDoc() > r.numDocs());
    sr.close();

    assertU(commit("expungeDeletes","true"));

    sr = req("q","foo");
    r = r = sr.getSearcher().getIndexReader();
    assertEquals(r.maxDoc(), r.numDocs());  // no deletions
    assertEquals(4,r.maxDoc());             // no dups
    sr.close();
  }
  
  @Test
  public void testPrepareCommit() throws Exception {
    assertU(adoc("id", "999"));
    assertU(optimize());     // make sure there's just one segment
    assertU(commit());       // commit a second time to make sure index files aren't still referenced by the old searcher

    SolrQueryRequest sr = req();
    DirectoryReader r = sr.getSearcher().getIndexReader();
    Directory d = r.directory();

    log.info("FILES before addDoc="+ Arrays.asList(d.listAll()));
    assertU(adoc("id", "1"));

    int nFiles = d.listAll().length;
    log.info("FILES before prepareCommit="+ Arrays.asList(d.listAll()));

    updateJ("", params("prepareCommit", "true"));

    log.info("FILES after prepareCommit="+Arrays.asList(d.listAll()));
    assertTrue( d.listAll().length > nFiles);  // make sure new index files were actually written
    
    assertJQ(req("q", "id:1")
        , "/response/numFound==0"
    );

    updateJ("", params("rollback","true"));
    assertU(commit());

    assertJQ(req("q", "id:1")
        , "/response/numFound==0"
    );

    assertU(adoc("id","1"));
    updateJ("", params("prepareCommit","true"));

    assertJQ(req("q", "id:1")
        , "/response/numFound==0"
    );

    assertU(commit());

    assertJQ(req("q", "id:1")
        , "/response/numFound==1"
    );

    sr.close();
  }

  @Test
  public void testPostSoftCommitEvents() throws Exception {
    SolrCore core = h.getCore();
    assert core != null;
    DirectUpdateHandler2 updater = (DirectUpdateHandler2) core.getUpdateHandler();
    MySolrEventListener listener = new MySolrEventListener();
    core.registerNewSearcherListener(listener);
    updater.registerSoftCommitCallback(listener);
    assertU(adoc("id", "999"));
    assertU(commit("softCommit", "true"));
    assertEquals("newSearcher was called more than once", 1, listener.newSearcherCount.get());
    assertFalse("postSoftCommit was not called", listener.postSoftCommitAt.get() == Long.MAX_VALUE);
    assertTrue("newSearcher was called after postSoftCommitCallback", listener.postSoftCommitAt.get() >= listener.newSearcherOpenedAt.get());
  }

  static class MySolrEventListener implements SolrEventListener {
    AtomicInteger newSearcherCount = new AtomicInteger(0);
    AtomicLong newSearcherOpenedAt = new AtomicLong(Long.MAX_VALUE);
    AtomicLong postSoftCommitAt = new AtomicLong(Long.MAX_VALUE);

    @Override
    public void postCommit() {
    }

    @Override
    public void postSoftCommit() {
      postSoftCommitAt.set(System.currentTimeMillis());
    }

    @Override
    public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
      newSearcherCount.incrementAndGet();
      newSearcherOpenedAt.set(newSearcher.getOpenTime());
    }

    @Override
    public void init(NamedList args) {

    }
  }
}
