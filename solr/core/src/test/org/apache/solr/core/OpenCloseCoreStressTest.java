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
package org.apache.solr.core;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Incorporate the open/close stress tests into unit tests.
 */
public class OpenCloseCoreStressTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Object locker = new Object();

  private int numCores = TEST_NIGHTLY ? 7 : 5;
  private Map<String, Long> coreCounts;
  private List<String> coreNames;

  static final int COMMIT_WITHIN = 5000;

  final int indexingThreads = TEST_NIGHTLY ? 9 : 5;
  final int queryThreads = TEST_NIGHTLY ? 9 : 5;

  static final int RESET_INTERVAL = 30 * 60; // minutes to report then delete everything
  long cumulativeDocs = 0;

  String url;

  JettySolrRunner jetty = null;

  File solrHomeDirectory;

  List<HttpSolrClient> indexingClients = new ArrayList<>(indexingThreads);
  List<HttpSolrClient> queryingClients = new ArrayList<>(queryThreads);

  static String savedFactory;
  
  @BeforeClass
  public static void beforeClass() {

  }
  
  @Before
  public void setupServer() throws Exception {
    coreCounts = new TreeMap<>();
    coreNames = new ArrayList<>();
    cumulativeDocs = 0;

    solrHomeDirectory = createTempDir().toFile();

    jetty = new JettySolrRunner(solrHomeDirectory.getAbsolutePath(), buildJettyConfig("/solr"));
  }

  @After
  public void tearDownServer() throws Exception {
    if (jetty != null) jetty.stop();
    IOUtils.close(indexingClients);
    IOUtils.close(queryingClients);
    indexingClients.clear();
    queryingClients.clear();
  }

  @Test
  public void test5Seconds() throws Exception {
    doStress(5);
  }
  
  @Test
  @Nightly
  public void test15Seconds() throws Exception {
    doStress(15);
  }

  @Test
  @Nightly
  public void test10Minutes() throws Exception {
    doStress(300);
  }

  @Test
  @Weekly
  public void test1Hour() throws Exception {
    doStress(1800);
  }
  
  private void buildClients() throws Exception {

    jetty.start();
    url = buildUrl(jetty.getLocalPort(), "/solr/");

    // Mostly to keep annoying logging messages from being sent out all the time.

    for (int idx = 0; idx < indexingThreads; ++idx) {
      HttpSolrClient client = getHttpSolrClient(url, 30000, 60000);
      indexingClients.add(client);
    }
    for (int idx = 0; idx < queryThreads; ++idx) {
      HttpSolrClient client = getHttpSolrClient(url, 30000, 30000);
      queryingClients.add(client);
    }

  }

  // Unless things go _really_ well, stop after you have the directories set up.
  private void doStress(int secondsToRun) throws Exception {
    makeCores(solrHomeDirectory);

    //MUST start the server after the cores are made.
    buildClients();

    try {

      log.info("Starting indexing and querying");

      int secondsRun = 0;
      int secondsRemaining = secondsToRun;
      do {

        int cycleSeconds = Math.min(RESET_INTERVAL, secondsRemaining);
        log.info(String.format(Locale.ROOT, "\n\n\n\n\nStarting a %,d second cycle, seconds left: %,d. Seconds run so far: %,d.",
            cycleSeconds, secondsRemaining, secondsRun));

        Indexer idxer = new Indexer(this, url, indexingClients, indexingThreads, cycleSeconds, random());

        Queries queries = new Queries(this, url, queryingClients, queryThreads, random());

        idxer.waitOnThreads();

        queries.waitOnThreads();

        secondsRemaining = Math.max(secondsRemaining - RESET_INTERVAL, 0);

        checkResults(queryingClients.get(0), queries, idxer);

        secondsRun += cycleSeconds;

        if (secondsRemaining > 0) {
          deleteAllDocuments(queryingClients.get(0), queries);
        }
      } while (secondsRemaining > 0);

      assertTrue("We didn't index any documents, somethings really messed up", cumulativeDocs > 0);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Caught unexpected exception");
    }
  }

  private void makeCores(File home) throws Exception {
    File testSrcRoot = new File(SolrTestCaseJ4.TEST_HOME());
    String srcSolrXml = "solr-stress-new.xml";

    FileUtils.copyFile(new File(testSrcRoot, srcSolrXml), new File(home, "solr.xml"));

    // create directories in groups of 100 until you have enough.
    for (int idx = 0; idx < numCores; ++idx) {
      String coreName = String.format(Locale.ROOT, "%05d_core", idx);
      makeCore(new File(home, coreName), testSrcRoot);
      coreCounts.put(coreName, 0L);
      coreNames.add(coreName);
    }
  }

  private void makeCore(File coreDir, File testSrcRoot) throws IOException {
    File conf = new File(coreDir, "conf");

    if (!conf.mkdirs()) log.warn("mkdirs returned false in makeCore... ignoring");

    File testConf = new File(testSrcRoot, "collection1/conf");

    FileUtils.copyFile(new File(testConf, "schema-tiny.xml"), new File(conf, "schema-tiny.xml"));

    FileUtils.copyFile(new File(testConf, "solrconfig-minimal.xml"), new File(conf, "solrconfig-minimal.xml"));
    FileUtils.copyFile(new File(testConf, "solrconfig.snippet.randomindexconfig.xml"),
        new File(conf, "solrconfig.snippet.randomindexconfig.xml"));

    FileUtils.copyFile(new File(testSrcRoot, "conf/core.properties"), new File(coreDir, "core.properties"));

  }


  void deleteAllDocuments(HttpSolrClient client, Queries queries) {
    log.info("Deleting data from last cycle, this may take a few minutes.");

    for (String core : coreNames) {
      try {
        client.setBaseURL(url + core);
        client.deleteByQuery("*:*");
        client.optimize(true, true); // should be close to a no-op.
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    // We're testing, after all. Let's be really sure things are as we expect.
    log.info("Insuring all cores empty");
    long foundDocs = 0;
    for (String core : coreNames) {
      try {
        long found = queries.getCount(client, core);
        assertEquals("Cores should be empty", found, 0L);
        foundDocs += found;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    if (foundDocs > 0) {
      log.warn("Found docs after purging done, this is bad.");
    }
    // Reset counters for another go-round
    coreCounts.clear();
    for (String core : coreNames) {
      coreCounts.put(core, 0L);
    }
  }

  private void checkResults(HttpSolrClient client, Queries queries, Indexer idxer) throws InterruptedException {
    log.info("Checking if indexes have all the documents they should...");
    long totalDocsFound = 0;
    for (Map.Entry<String, Long> ent : coreCounts.entrySet()) {
      client.setBaseURL(url + ent.getKey());
      for (int idx = 0; idx < 3; ++idx) {
        try {
          client.commit(true, true);
          break; // retry loop
        } catch (Exception e) {
          log.warn("Exception when committing core " + ent.getKey() + " " + e.getMessage());
          Thread.sleep(100L);
        }
      }
      long numFound = queries.getCount(client, ent.getKey());
      totalDocsFound += numFound;
      assertEquals(String.format(Locale.ROOT, "Core %s bad!", ent.getKey()), (long) ent.getValue(), numFound);
    }

    log.info(String.format(Locale.ROOT, "\n\nDocs indexed (cumulative, all cycles): %,d, total docs: %,d: Cycle stats: updates: %,d: qtimes: %,d",
        Indexer.idUnique.get(), totalDocsFound, idxer.getAccumUpdates(), idxer.getAccumQtimes()));

    cumulativeDocs += totalDocsFound;
  }

  String getRandomCore(Random random) {
    return coreNames.get(Math.abs(random.nextInt()) % coreNames.size());
  }

  void incrementCoreCount(String core) {
    synchronized (locker) {
      coreCounts.put(core, coreCounts.get(core) + 1);
    }
  }
}

class Indexer {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static AtomicInteger idUnique = new AtomicInteger(0);

  static AtomicInteger errors = new AtomicInteger(0);

  static AtomicInteger docsThisCycle = new AtomicInteger(0);

  static AtomicLong qTimesAccum = new AtomicLong(0);

  static AtomicInteger updateCounts = new AtomicInteger(0);

  static volatile int lastCount;

  static volatile TimeOut stopTimeout;
  private static volatile TimeOut nextTimeout;

  ArrayList<OneIndexer> _threads = new ArrayList<>();

  public Indexer(OpenCloseCoreStressTest OCCST, String url, List<HttpSolrClient> clients, int numThreads, int secondsToRun, Random random) {
    stopTimeout = new TimeOut(secondsToRun, TimeUnit.SECONDS);
    nextTimeout = new TimeOut(60, TimeUnit.SECONDS);
    docsThisCycle.set(0);
    qTimesAccum.set(0);
    updateCounts.set(0);
    for (int idx = 0; idx < numThreads; ++idx) {
      OneIndexer one = new OneIndexer(OCCST, url, clients.get(idx), random.nextLong());
      _threads.add(one);
      one.start();
    }
  }

  public void waitOnThreads() {
    for (Thread thread : _threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public long getAccumQtimes() {
    return qTimesAccum.get();
  }

  public int getAccumUpdates() {
    return updateCounts.get();
  }

  synchronized static void progress(int myId, String core) {
    if (nextTimeout.hasTimedOut()) {
      log.info(String.format(Locale.ROOT, " s indexed: [run %,8d] [cycle %,8d] [last minute %,8d] Last core updated: %s. Seconds left in cycle %,4d",
          myId, docsThisCycle.get(), myId - lastCount, core, stopTimeout.timeLeft(TimeUnit.SECONDS)));
      lastCount = myId;
      nextTimeout = new TimeOut(60, TimeUnit.SECONDS);
    }
  }

}

class OneIndexer extends Thread {
  private final OpenCloseCoreStressTest OCCST;
  private final HttpSolrClient client;
  private final String baseUrl;
  private final Random random;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  OneIndexer(OpenCloseCoreStressTest OCCST, String url, HttpSolrClient client, long seed) {
    this.OCCST = OCCST;
    this.client = client;
    this.baseUrl = url;
    this.random = new Random(seed);
  }

  @Override
  public void run() {
    log.info(String.format(Locale.ROOT, "Starting indexing thread: " + getId()));

    while (! Indexer.stopTimeout.hasTimedOut()) {
      int myId = Indexer.idUnique.incrementAndGet();
      Indexer.docsThisCycle.incrementAndGet();
      String core = OCCST.getRandomCore(random);
      OCCST.incrementCoreCount(core);
      Indexer.progress(myId, core);
      for (int idx = 0; idx < 3; ++idx) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "id" + Integer.toString(myId));
        doc.addField("text", "text " + Integer.toString(myId));
        UpdateRequest update = new UpdateRequest();
        update.add(doc);

        try {
          client.setBaseURL(baseUrl + core);
          UpdateResponse response = client.add(doc, OpenCloseCoreStressTest.COMMIT_WITHIN);
          if (response.getStatus() != 0) {
            log.warn("Failed to index a document to core " + core + " with status " + response.getStatus());
          } else {
            Indexer.qTimesAccum.addAndGet(response.getQTime());
            Indexer.updateCounts.incrementAndGet();
            break; // retry loop.
          }
          Thread.sleep(100L); // Let's not go crazy here.
        } catch (Exception e) {
          if (e instanceof InterruptedException) return;
          Indexer.errors.incrementAndGet();
          if (idx == 2) {
            log.warn("Could not reach server while indexing for three tries, quitting " + e.getMessage());
          } else {
            log.info("Indexing thread " + Thread.currentThread().getId() + " swallowed one exception " + e.getMessage());
            try {
              Thread.sleep(500);
            } catch (InterruptedException tex) {
              return;
            }
          }
        }
      }
    }
    log.info("Leaving indexing thread " + getId());
  }
}

class Queries {
  static AtomicBoolean _keepon = new AtomicBoolean(true);

  List<Thread> _threads = new ArrayList<>();
  static AtomicInteger _errors = new AtomicInteger(0);
  String baseUrl;

  public Queries(OpenCloseCoreStressTest OCCST, String url, List<HttpSolrClient> clients, int numThreads, Random random) {
    baseUrl = url;
    for (int idx = 0; idx < numThreads; ++idx) {
      Thread one = new OneQuery(OCCST, url, clients.get(idx), random.nextLong());
      _threads.add(one);
      one.start();
    }

  }

  public void waitOnThreads() {
    Queries._keepon.set(false);
    for (Thread thread : _threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public long getCount(HttpSolrClient client, String core) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", "/select");
    params.set("q", "*:*");
    long numFound = 0;
    client.setBaseURL(baseUrl + core);
    try {
      QueryResponse response = client.query(params);
      numFound = response.getResults().getNumFound();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return numFound;
  }
}

class OneQuery extends Thread {
  OpenCloseCoreStressTest OCCST;
  private final HttpSolrClient client;
  private final String baseUrl;
  private final Random random;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  OneQuery(OpenCloseCoreStressTest OCCST, String url, HttpSolrClient client, long seed) {
    this.OCCST = OCCST;
    this.client = client;
    this.baseUrl = url;
    this.random = new Random(seed);
  }

  @Override
  public void run() {
    log.info(String.format(Locale.ROOT, "Starting query thread: " + getId()));
    while (Queries._keepon.get()) {
      String core = OCCST.getRandomCore(random);
      for (int idx = 0; idx < 3; ++idx) {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("qt", "/select");
        params.set("q", "*:*");

        try {
          // sleep between 250ms and 10000 ms
          Thread.sleep(100L); // Let's not go crazy here.
          client.setBaseURL(baseUrl + core);
          QueryResponse response = client.query(params);

          if (response.getStatus() != 0) {
            log.warn("Failed to query core " + core + " with status " + response.getStatus());
          }
            // Perhaps collect some stats here in future.
          break; // retry loop
        } catch (Exception e) {
          if (e instanceof InterruptedException) return;
          Queries._errors.incrementAndGet();
          if (idx == 2) {
            log.warn("Could not reach server while indexing for three tries, quitting " + e.getMessage());
          } else {
            log.info("Querying thread: " + Thread.currentThread().getId() + " swallowed exception: " + e.getMessage());
            try {
              Thread.sleep(500L);
            } catch (InterruptedException tex) {
              return;
            }
          }
        }
      }
    }
    log.info(String.format(Locale.ROOT, "Leaving query thread: " + getId()));
  }

}
