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

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.index.TieredMergePolicyFactory;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.LogLevel;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.VERSION_FIELD;

@LogLevel("org.apache.solr.update=INFO")
public class DirectUpdateHandlerTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static String savedFactory;
  @BeforeClass
  public static void beforeClass() throws Exception {
    savedFactory = System.getProperty("solr.DirectoryFactory");
    System.setProperty("solr.directoryFactory", "org.apache.solr.core.MockFSDirectoryFactory");
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    systemSetPropertySolrTestsMergePolicyFactory(TieredMergePolicyFactory.class.getName());
    initCore("solrconfig.xml", "schema12.xml");
  }
  
  @AfterClass
  public static void afterClass() {
    systemClearPropertySolrTestsMergePolicyFactory();
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
  @SuppressWarnings({"unchecked"})
  public void testBasics() throws Exception {

    // get initial metrics
    Map<String, Metric> metrics = h.getCoreContainer().getMetricManager()
        .registry(h.getCore().getCoreMetricManager().getRegistryName()).getMetrics();

    String PREFIX = "UPDATE.updateHandler.";

    String commitsName = PREFIX + "commits";
    assertTrue(metrics.containsKey(commitsName));
    String addsName = PREFIX + "adds";
    assertTrue(metrics.containsKey(addsName));
    String cumulativeAddsName = PREFIX + "cumulativeAdds";
    String delsIName = PREFIX + "deletesById";
    String cumulativeDelsIName = PREFIX + "cumulativeDeletesById";
    String delsQName = PREFIX + "deletesByQuery";
    String cumulativeDelsQName = PREFIX + "cumulativeDeletesByQuery";
    long commits = ((Meter) metrics.get(commitsName)).getCount();
    long adds = ((Gauge<Number>) metrics.get(addsName)).getValue().longValue();
    long cumulativeAdds = ((Meter) metrics.get(cumulativeAddsName)).getCount();
    long cumulativeDelsI = ((Meter) metrics.get(cumulativeDelsIName)).getCount();
    long cumulativeDelsQ = ((Meter) metrics.get(cumulativeDelsQName)).getCount();


    assertNull("This test requires a schema that has no version field, " +
               "it appears the schema file in use has been edited to violate " +
               "this requirement",
               h.getCore().getLatestSchema().getFieldOrNull(VERSION_FIELD));

    assertU(adoc("id","5"));
    assertU(adoc("id","6"));

    // search - not committed - docs should not be found.
    assertQ(req("q","id:5"), "//*[@numFound='0']");
    assertQ(req("q","id:6"), "//*[@numFound='0']");

    long newAdds = ((Gauge<Number>) metrics.get(addsName)).getValue().longValue();
    long newCumulativeAdds = ((Meter) metrics.get(cumulativeAddsName)).getCount();
    assertEquals("new adds", 2, newAdds - adds);
    assertEquals("new cumulative adds", 2, newCumulativeAdds - cumulativeAdds);

    assertU(commit());

    long newCommits = ((Meter) metrics.get(commitsName)).getCount();
    assertEquals("new commits", 1, newCommits - commits);

    newAdds = ((Gauge<Number>) metrics.get(addsName)).getValue().longValue();
    newCumulativeAdds = ((Meter) metrics.get(cumulativeAddsName)).getCount();
    // adds should be reset to 0 after commit
    assertEquals("new adds after commit", 0, newAdds);
    // not so with cumulative ones!
    assertEquals("new cumulative adds after commit", 2, newCumulativeAdds - cumulativeAdds);

    // now they should be there
    assertQ(req("q","id:5"), "//*[@numFound='1']");
    assertQ(req("q","id:6"), "//*[@numFound='1']");

    // now delete one
    assertU(delI("5"));

    long newDelsI = ((Gauge<Number>) metrics.get(delsIName)).getValue().longValue();
    long newCumulativeDelsI = ((Meter) metrics.get(cumulativeDelsIName)).getCount();
    assertEquals("new delsI", 1, newDelsI);
    assertEquals("new cumulative delsI", 1, newCumulativeDelsI - cumulativeDelsI);

    // not committed yet
    assertQ(req("q","id:5"), "//*[@numFound='1']");

    assertU(commit());
    // delsI should be reset to 0 after commit
    newDelsI = ((Gauge<Number>) metrics.get(delsIName)).getValue().longValue();
    newCumulativeDelsI = ((Meter) metrics.get(cumulativeDelsIName)).getCount();
    assertEquals("new delsI after commit", 0, newDelsI);
    assertEquals("new cumulative delsI after commit", 1, newCumulativeDelsI - cumulativeDelsI);

    // 5 should be gone
    assertQ(req("q","id:5"), "//*[@numFound='0']");
    assertQ(req("q","id:6"), "//*[@numFound='1']");

    // now delete all
    assertU(delQ("*:*"));

    long newDelsQ = ((Gauge<Number>) metrics.get(delsQName)).getValue().longValue();
    long newCumulativeDelsQ = ((Meter) metrics.get(cumulativeDelsQName)).getCount();
    assertEquals("new delsQ", 1, newDelsQ);
    assertEquals("new cumulative delsQ", 1, newCumulativeDelsQ - cumulativeDelsQ);

    // not committed yet
    assertQ(req("q","id:6"), "//*[@numFound='1']");

    assertU(commit());

    newDelsQ = ((Gauge<Number>) metrics.get(delsQName)).getValue().longValue();
    newCumulativeDelsQ = ((Meter) metrics.get(cumulativeDelsQName)).getCount();
    assertEquals("new delsQ after commit", 0, newDelsQ);
    assertEquals("new cumulative delsQ after commit", 1, newCumulativeDelsQ - cumulativeDelsQ);

    // 6 should be gone
    assertQ(req("q","id:6"), "//*[@numFound='0']");

    // verify final metrics
    newCommits = ((Meter) metrics.get(commitsName)).getCount();
    assertEquals("new commits", 3, newCommits - commits);
    newAdds = ((Gauge<Number>) metrics.get(addsName)).getValue().longValue();
    assertEquals("new adds", 0, newAdds);
    newCumulativeAdds = ((Meter) metrics.get(cumulativeAddsName)).getCount();
    assertEquals("new cumulative adds", 2, newCumulativeAdds - cumulativeAdds);
    newDelsI = ((Gauge<Number>) metrics.get(delsIName)).getValue().longValue();
    assertEquals("new delsI", 0, newDelsI);
    newCumulativeDelsI = ((Meter) metrics.get(cumulativeDelsIName)).getCount();
    assertEquals("new cumulative delsI", 1, newCumulativeDelsI - cumulativeDelsI);

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
    assertEquals( 1, duh2.addCommands.longValue() );
    assertEquals( 1, duh2.addCommandsCumulative.getCount() );
    assertEquals( 0, duh2.commitCommands.getCount() );
    updater.commit(cmtCmd);
    assertEquals( 0, duh2.addCommands.longValue() );
    assertEquals( 1, duh2.addCommandsCumulative.getCount() );
    assertEquals( 1, duh2.commitCommands.getCount() );
    ureq.close();

    assertU(adoc("id","B"));

    // rollback "B"
    ureq = req();
    RollbackUpdateCommand rbkCmd = new RollbackUpdateCommand(ureq);
    assertEquals( 1, duh2.addCommands.longValue() );
    assertEquals( 2, duh2.addCommandsCumulative.getCount() );
    assertEquals( 0, duh2.rollbackCommands.getCount() );
    updater.rollback(rbkCmd);
    assertEquals( 0, duh2.addCommands.longValue() );
    assertEquals( 1, duh2.addCommandsCumulative.getCount() );
    assertEquals( 1, duh2.rollbackCommands.getCount() );
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
    assertEquals( 2, duh2.addCommands.longValue() );
    assertEquals( 2, duh2.addCommandsCumulative.getCount() );
    assertEquals( 0, duh2.commitCommands.getCount() );
    updater.commit(cmtCmd);
    assertEquals( 0, duh2.addCommands.longValue() );
    assertEquals( 2, duh2.addCommandsCumulative.getCount() );
    assertEquals( 1, duh2.commitCommands.getCount() );
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
    assertEquals( 1, duh2.deleteByIdCommands.longValue() );
    assertEquals( 1, duh2.deleteByIdCommandsCumulative.getCount() );
    assertEquals( 0, duh2.rollbackCommands.getCount() );
    updater.rollback(rbkCmd);
    ureq.close();
    assertEquals( 0, duh2.deleteByIdCommands.longValue() );
    assertEquals( 0, duh2.deleteByIdCommandsCumulative.getCount() );
    assertEquals( 1, duh2.rollbackCommands.getCount() );
    
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
    r = sr.getSearcher().getIndexReader();
    assertEquals(r.maxDoc(), r.numDocs());  // no deletions
    assertEquals(4,r.maxDoc());             // no dups
    sr.close();
  }
  
  @Test
  public void testPrepareCommit() throws Exception {
    assertU(adoc("id", "999"));
    assertU(optimize("maxSegments", "1"));     // make sure there's just one segment
    assertU(commit());       // commit a second time to make sure index files aren't still referenced by the old searcher

    SolrQueryRequest sr = req();
    DirectoryReader r = sr.getSearcher().getIndexReader();
    Directory d = r.directory();

    if (log.isInfoEnabled()) {
      log.info("FILES before addDoc={}", Arrays.asList(d.listAll()));
    }
    assertU(adoc("id", "1"));

    assertFalse(Arrays.stream(d.listAll()).anyMatch(s -> s.startsWith(IndexFileNames.PENDING_SEGMENTS)));
    String beforeSegmentsFile =
        Arrays.stream(d.listAll()).filter(s -> s.startsWith(IndexFileNames.SEGMENTS)).findAny().get();

    if (log.isInfoEnabled()) {
      log.info("FILES before prepareCommit={}", Arrays.asList(d.listAll()));
    }

    updateJ("", params("prepareCommit", "true"));

    if (log.isInfoEnabled()) {
      log.info("FILES after prepareCommit={}", Arrays.asList(d.listAll()));
    }
    assertTrue(Arrays.stream(d.listAll()).anyMatch(s -> s.startsWith(IndexFileNames.PENDING_SEGMENTS)));
    assertEquals(beforeSegmentsFile,
        Arrays.stream(d.listAll()).filter(s -> s.startsWith(IndexFileNames.SEGMENTS)).findAny().get());

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
      postSoftCommitAt.set(System.nanoTime());
    }

    @Override
    public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
      newSearcherCount.incrementAndGet();
      newSearcherOpenedAt.set(newSearcher.getOpenNanoTime());
    }

    @Override
    public void init(@SuppressWarnings({"rawtypes"})NamedList args) {

    }
  }
}
