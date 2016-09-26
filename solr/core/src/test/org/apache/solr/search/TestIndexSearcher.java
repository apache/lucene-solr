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
package org.apache.solr.search;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.index.LogDocMergePolicyFactory;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.servlet.DirectSolrConnection;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestIndexSearcher extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {

    // we need a consistent segmentation because reopen test validation
    // dependso n merges not happening when it doesn't expect
    systemSetPropertySolrTestsMergePolicy(LogDocMergePolicy.class.getName());
    systemSetPropertySolrTestsMergePolicyFactory(LogDocMergePolicyFactory.class.getName());

    initCore("solrconfig.xml","schema.xml");
  }
  
  @AfterClass
  public static void afterClass() {
    systemClearPropertySolrTestsMergePolicy();
    systemClearPropertySolrTestsMergePolicyFactory();
  }

  @Override
  public void setUp() throws Exception {
    System.getProperties().remove("tests.solr.useColdSearcher");
    super.setUp();
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    assertU(delQ("*:*"));
    assertU((commit()));
  }

  private String getStringVal(SolrQueryRequest sqr, String field, int doc) throws IOException {
    SchemaField sf = sqr.getSchema().getField(field);
    ValueSource vs = sf.getType().getValueSource(sf, null);
    Map context = ValueSource.newContext(sqr.getSearcher());
    vs.createWeight(context, sqr.getSearcher());
    IndexReaderContext topReaderContext = sqr.getSearcher().getTopReaderContext();
    List<LeafReaderContext> leaves = topReaderContext.leaves();
    int idx = ReaderUtil.subIndex(doc, leaves);
    LeafReaderContext leaf = leaves.get(idx);
    FunctionValues vals = vs.getValues(context, leaf);
    return vals.strVal(doc-leaf.docBase);
  }

  public void testReopen() throws Exception {

    assertU(adoc("id","1", "v_t","Hello Dude", "v_s1","string1"));
    assertU(adoc("id","2", "v_t","Hello Yonik", "v_s1","string2"));
    assertU(commit());

    SolrQueryRequest sr1 = req("q","foo");
    IndexReader r1 = sr1.getSearcher().getRawReader();

    String sval1 = getStringVal(sr1, "v_s1",0);
    assertEquals("string1", sval1);

    assertU(adoc("id","3", "v_s1","{!literal}"));
    assertU(adoc("id","4", "v_s1","other stuff"));
    assertU(commit());

    SolrQueryRequest sr2 = req("q","foo");
    IndexReader r2 = sr2.getSearcher().getRawReader();

    // make sure the readers share the first segment
    // Didn't work w/ older versions of lucene2.9 going from segment -> multi
    assertEquals(r1.leaves().get(0).reader(), r2.leaves().get(0).reader());

    assertU(adoc("id","5", "v_f","3.14159"));
    assertU(adoc("id","6", "v_f","8983", "v_s1","string6"));
    assertU(commit());

    SolrQueryRequest sr3 = req("q","foo");
    IndexReader r3 = sr3.getSearcher().getRawReader();
    // make sure the readers share segments
    // assertEquals(r1.getLeafReaders()[0], r3.getLeafReaders()[0]);
    assertEquals(r2.leaves().get(0).reader(), r3.leaves().get(0).reader());
    assertEquals(r2.leaves().get(1).reader(), r3.leaves().get(1).reader());

    sr1.close();
    sr2.close();            

    // should currently be 1, but this could change depending on future index management
    int baseRefCount = r3.getRefCount();
    assertEquals(1, baseRefCount);

    Object sr3SearcherRegAt = sr3.getSearcher().getStatistics().get("registeredAt");
    assertU(commit()); // nothing has changed
    SolrQueryRequest sr4 = req("q","foo");
    assertSame("nothing changed, searcher should be the same",
               sr3.getSearcher(), sr4.getSearcher());
    assertEquals("nothing changed, searcher should not have been re-registered",
                 sr3SearcherRegAt, sr4.getSearcher().getStatistics().get("registeredAt"));
    IndexReader r4 = sr4.getSearcher().getRawReader();

    // force an index change so the registered searcher won't be the one we are testing (and
    // then we should be able to test the refCount going all the way to 0
    assertU(adoc("id","7", "v_f","7574"));
    assertU(commit()); 

    // test that reader didn't change
    assertSame(r3, r4);
    assertEquals(baseRefCount, r4.getRefCount());
    sr3.close();
    assertEquals(baseRefCount, r4.getRefCount());
    sr4.close();
    assertEquals(baseRefCount-1, r4.getRefCount());


    SolrQueryRequest sr5 = req("q","foo");
    IndexReaderContext rCtx5 = sr5.getSearcher().getTopReaderContext();

    assertU(delI("1"));
    assertU(commit());
    SolrQueryRequest sr6 = req("q","foo");
    IndexReaderContext rCtx6 = sr6.getSearcher().getTopReaderContext();
    assertEquals(1, rCtx6.leaves().get(0).reader().numDocs()); // only a single doc left in the first segment
    assertTrue( !rCtx5.leaves().get(0).reader().equals(rCtx6.leaves().get(0).reader()) );  // readers now different

    sr5.close();
    sr6.close();
  }


  // make sure we don't leak searchers (SOLR-3391)
  public void testCloses() {
    assertU(adoc("id","1"));
    assertU(commit("openSearcher","false"));  // this was enough to trigger SOLR-3391

    int maxDoc = random().nextInt(20) + 1;

    // test different combinations of commits
    for (int i=0; i<100; i++) {

      if (random().nextInt(100) < 50) {
        String id = Integer.toString(random().nextInt(maxDoc));
        assertU(adoc("id",id));
      } else {
        boolean soft = random().nextBoolean();
        boolean optimize = random().nextBoolean();
        boolean openSearcher = random().nextBoolean();

        if (optimize) {
          assertU(optimize("openSearcher",""+openSearcher, "softCommit",""+soft));
        } else {
          assertU(commit("openSearcher",""+openSearcher, "softCommit",""+soft));
        }
      }
    }

  }
  
  public void testSearcherListeners() throws Exception {
    MockSearchComponent.registerSlowSearcherListener = false;
        
    MockSearchComponent.registerFirstSearcherListener = false;
    MockSearchComponent.registerNewSearcherListener = false;
    createCoreAndValidateListeners(0, 0, 0, 0);
    
    MockSearchComponent.registerFirstSearcherListener = true;
    MockSearchComponent.registerNewSearcherListener = false;
    createCoreAndValidateListeners(1, 1, 1, 1);
    
    MockSearchComponent.registerFirstSearcherListener = true;
    MockSearchComponent.registerNewSearcherListener = true;
    createCoreAndValidateListeners(1, 1, 2, 1);
  }
  
  private void createCoreAndValidateListeners(int numTimesCalled, int numTimesCalledFirstSearcher,
      int numTimesCalledAfterGetSearcher, int numTimesCalledFirstSearcherAfterGetSearcher) throws Exception {
    CoreContainer cores = h.getCoreContainer();
    CoreDescriptor cd = h.getCore().getCoreDescriptor();
    SolrCore newCore = null;
    // reset counters
    MockSearcherListener.numberOfTimesCalled = new AtomicInteger();
    MockSearcherListener.numberOfTimesCalledFirstSearcher = new AtomicInteger();
    
    try {
      // Create a new core, this should call all the firstSearcherListeners
      newCore = cores.create("core1", cd.getInstanceDir(), ImmutableMap.of("config", "solrconfig-searcher-listeners1.xml"), false);
      
      //validate that the new core was created with the correct solrconfig
      assertNotNull(newCore.getSearchComponent("mock"));
      assertEquals(MockSearchComponent.class, newCore.getSearchComponent("mock").getClass());
      assertFalse(newCore.getSolrConfig().useColdSearcher);
      
      doQuery(newCore);
      
      assertEquals(numTimesCalled, MockSearcherListener.numberOfTimesCalled.get());
      assertEquals(numTimesCalledFirstSearcher, MockSearcherListener.numberOfTimesCalledFirstSearcher.get());
      
      addDummyDoc(newCore);
      
      // Open a new searcher, this should call the newSearcherListeners
      Future<?>[] future = new Future[1];
      newCore.getSearcher(true, false, future);
      future[0].get();
      
      assertEquals(numTimesCalledAfterGetSearcher, MockSearcherListener.numberOfTimesCalled.get());
      assertEquals(numTimesCalledFirstSearcherAfterGetSearcher, MockSearcherListener.numberOfTimesCalledFirstSearcher.get());
      
    } finally {
      if (newCore != null) {
        cores.unload("core1");
      }
    }
  }
  
  private void doQuery(SolrCore core) throws Exception {
    DirectSolrConnection connection = new DirectSolrConnection(core);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    assertTrue(connection.request("/select",params, null ).contains("<int name=\"status\">0</int>"));
  }

  public void testDontUseColdSearcher() throws Exception {
    MockSearchComponent.registerFirstSearcherListener = false;
    MockSearchComponent.registerNewSearcherListener = false;
    MockSearchComponent.registerSlowSearcherListener = true;
    final AtomicBoolean querySucceeded = new AtomicBoolean(false);
    SlowSearcherListener.numberOfTimesCalled = new AtomicInteger(0);
    SlowSearcherListener.latch = new CountDownLatch(1);
    
    CoreContainer cores = h.getCoreContainer();
    CoreDescriptor cd = h.getCore().getCoreDescriptor();
    final SolrCore newCore;
    boolean coreCreated = false;
    try {
      // Create a new core, this should call all the firstSearcherListeners
      newCore = cores.create("core1", cd.getInstanceDir(), ImmutableMap.of("config", "solrconfig-searcher-listeners1.xml"), false);
      coreCreated = true;
      
      //validate that the new core was created with the correct solrconfig
      assertNotNull(newCore.getSearchComponent("mock"));
      assertEquals(MockSearchComponent.class, newCore.getSearchComponent("mock").getClass());
      assertFalse(newCore.getSolrConfig().useColdSearcher);
      
      Thread t = new Thread() {
        public void run() {
          try {
            doQuery(newCore);
            querySucceeded.set(true);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };
      };
      t.start();
      
      if (System.getProperty(SYSPROP_NIGHTLY) != null) {
        // even if we wait here, the SearcherListener should not finish
        Thread.sleep(500);
      }
      // validate that the searcher warmer didn't finish yet. 
      assertEquals(0, SlowSearcherListener.numberOfTimesCalled.get());
      assertFalse("Query should be waiting for warming to finish", querySucceeded.get());
      
      // Let warmer finish 
      SlowSearcherListener.latch.countDown();
      
      // Validate that the query eventually succeeds
      for (int i = 0; i <= 1000; i++) {
        if (querySucceeded.get()) {
          break;
        }
        if (i == 1000) {
          fail("Query didn't succeed after 10 secoonds");
        }
        Thread.sleep(10);
      }
      
    } finally {
      
      if (coreCreated) {
        cores.unload("core1");
      }
    }
  }
  
  public void testUseColdSearcher() throws Exception {
    MockSearchComponent.registerFirstSearcherListener = false;
    MockSearchComponent.registerNewSearcherListener = false;
    MockSearchComponent.registerSlowSearcherListener = true;
    final AtomicBoolean querySucceeded = new AtomicBoolean(false);
    SlowSearcherListener.numberOfTimesCalled = new AtomicInteger(0);
    SlowSearcherListener.latch = new CountDownLatch(1);
    
    
    CoreContainer cores = h.getCoreContainer();
    CoreDescriptor cd = h.getCore().getCoreDescriptor();
    final SolrCore newCore;
    boolean coreCreated = false;
    try {
      System.setProperty("tests.solr.useColdSearcher", "true");
      // Create a new core, this should call all the firstSearcherListeners
      newCore = cores.create("core1", cd.getInstanceDir(), ImmutableMap.of("config", "solrconfig-searcher-listeners1.xml"), false);
      coreCreated = true;
      
      //validate that the new core was created with the correct solrconfig
      assertNotNull(newCore.getSearchComponent("mock"));
      assertEquals(MockSearchComponent.class, newCore.getSearchComponent("mock").getClass());
      assertTrue(newCore.getSolrConfig().useColdSearcher);
      
      Thread t = new Thread() {
        public void run() {
          try {
            doQuery(newCore);
            querySucceeded.set(true);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };
      };
      t.start();
      
      // validate that the query runs before the searcher warmer finishes
      for (int i = 0; i <= 1000; i++) {
        if (querySucceeded.get()) {
          break;
        }
        if (i == 1000) {
          fail("Query didn't succeed after 10 secoonds");
        }
        Thread.sleep(10);
      }
      
      assertEquals(0, SlowSearcherListener.numberOfTimesCalled.get());
      
    } finally {
      System.getProperties().remove("tests.solr.useColdSearcher");
      if (coreCreated) {
        SlowSearcherListener.latch.countDown();
        cores.unload("core1");
      }
      
    }
  }

  private void addDummyDoc(SolrCore core) throws Exception {
    DirectSolrConnection connection = new DirectSolrConnection(core);
    SolrRequestHandler handler = core.getRequestHandler("/update");
    connection.request(handler, null, adoc("id", "1"));
  }

  public static class MockSearchComponent extends SearchComponent implements SolrCoreAware {

    static boolean registerFirstSearcherListener = false;
    static boolean registerNewSearcherListener = false;
    static boolean registerSlowSearcherListener = false;
    
    @Override
    public void prepare(ResponseBuilder rb) throws IOException {}

    @Override
    public void process(ResponseBuilder rb) throws IOException {}

    @Override
    public String getDescription() {
      return "MockSearchComponent";
    }

    @Override
    public void inform(SolrCore core) {
      if (registerFirstSearcherListener) {
        core.registerFirstSearcherListener(new MockSearcherListener());
      }
      if (registerNewSearcherListener) {
        core.registerNewSearcherListener(new MockSearcherListener());
      }
      if (registerSlowSearcherListener) {
        core.registerFirstSearcherListener(new SlowSearcherListener());
      }
    }
    
  }
  
  static class MockSearcherListener implements SolrEventListener {
    
    static AtomicInteger numberOfTimesCalled;
    static AtomicInteger numberOfTimesCalledFirstSearcher;

    @Override
    public void init(NamedList args) {}

    @Override
    public void postCommit() {}

    @Override
    public void postSoftCommit() {}

    @Override
    public void newSearcher(SolrIndexSearcher newSearcher,
        SolrIndexSearcher currentSearcher) {
      numberOfTimesCalled.incrementAndGet();
      if (currentSearcher == null) {
        numberOfTimesCalledFirstSearcher.incrementAndGet();
      }
    }
  }
  
  static class SlowSearcherListener implements SolrEventListener {
    
    static AtomicInteger numberOfTimesCalled;
    static CountDownLatch latch;
    
    @Override
    public void init(NamedList args) {}

    @Override
    public void postCommit() {}

    @Override
    public void postSoftCommit() {}

    @Override
    public void newSearcher(SolrIndexSearcher newSearcher,
        SolrIndexSearcher currentSearcher) {
      try {
        assert currentSearcher == null: "SlowSearcherListener should only be used as FirstSearcherListener";
        // simulate a slow searcher listener
        latch.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      numberOfTimesCalled.incrementAndGet();
    }
  }
}
