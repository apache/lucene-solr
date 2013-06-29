package org.apache.solr.core;

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

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Incorporate the open/close stress tests into unit tests.
 */
public class OpenCloseCoreStressTest extends SolrTestCaseJ4 {

  private final Object locker = new Object();

  private int numCores = 20;
  private Map<String, Long> coreCounts;
  private List<String> coreNames;

  static final int COMMIT_WITHIN = 5000;

  final int indexingThreads = 15;
  final int queryThreads = 15;

  final int resetInterval = 30 * 60; // minutes to report then delete everything
  long cumulativeDocs = 0;

  String url;

  JettySolrRunner jetty = null;

  File solrHomeDirectory;

  List<HttpSolrServer> indexingServers = new ArrayList<HttpSolrServer>(indexingThreads);
  List<HttpSolrServer> queryServers = new ArrayList<HttpSolrServer>(queryThreads);

  static String savedFactory;

  //  Keep the indexes from being randomly generated.
  @BeforeClass
  public static void beforeClass() {
    savedFactory = System.getProperty("solr.DirectoryFactory");
    System.setProperty("solr.directoryFactory", "org.apache.solr.core.MockFSDirectoryFactory");
  }
  @AfterClass
  public static void afterClass() {
    if (savedFactory == null) {
      System.clearProperty("solr.directoryFactory");
    } else {
      System.setProperty("solr.directoryFactory", savedFactory);
    }
  }

  @Before
  public void setupServer() throws Exception {
    coreCounts = new TreeMap<String, Long>();
    coreNames = new ArrayList<String>();
    cumulativeDocs = 0;

    solrHomeDirectory = new File(TEMP_DIR, "OpenCloseCoreStressTest_");
    FileUtils.deleteDirectory(solrHomeDirectory); // Insure that a failed test didn't leave something lying around.

    jetty = new JettySolrRunner(solrHomeDirectory.getAbsolutePath(), "/solr", 0);
  }

  @After
  public void tearDownServer() throws Exception {
    if (jetty != null) jetty.stop();
    FileUtils.deleteDirectory(solrHomeDirectory);
  }

  @Test
  @Slow
  public void test30SecondsOld() throws Exception {
    doStress(30, true);
  }

  @Test
  @Slow
  public void test30SecondsNew() throws Exception {
    doStress(30, false);
  }

  @Test
  @Nightly
  public void test10MinutesOld() throws Exception {
    doStress(300, true);
  }

  @Test
  @Nightly
  public void test10MinutesNew() throws Exception {
    doStress(300, false);
  }

  @Test
  @Weekly
  public void test1HourOld() throws Exception {
    doStress(1800, true);
  }

  @Test
  @Weekly
  public void test1HourNew() throws Exception {
    doStress(1800, false);
  }


  private void getServers() throws Exception {
    jetty.start();
    url = "http://127.0.0.1:" + jetty.getLocalPort() + "/solr/";

    // Mostly to keep annoying logging messages from being sent out all the time.

    for (int idx = 0; idx < indexingThreads; ++idx) {
      HttpSolrServer server = new HttpSolrServer(url);
      server.setDefaultMaxConnectionsPerHost(25);
      server.setConnectionTimeout(30000);
      server.setSoTimeout(30000);
      indexingServers.add(server);
    }
    for (int idx = 0; idx < queryThreads; ++idx) {
      HttpSolrServer server = new HttpSolrServer(url);
      server.setDefaultMaxConnectionsPerHost(25);
      server.setConnectionTimeout(30000);
      server.setSoTimeout(30000);
      queryServers.add(server);
    }

  }

  // Unless things go _really_ well, stop after you have the directories set up.
  private void doStress(int secondsToRun, boolean oldStyle) throws Exception {
    makeCores(solrHomeDirectory, oldStyle);

    //MUST start the server after the cores are made.
    getServers();

    try {

      log.info("Starting indexing and querying");

      int secondsRun = 0;
      int secondsRemaining = secondsToRun;
      do {

        int cycleSeconds = Math.min(resetInterval, secondsRemaining);
        log.info(String.format(Locale.ROOT, "\n\n\n\n\nStarting a %,d second cycle, seconds left: %,d. Seconds run so far: %,d.",
            cycleSeconds, secondsRemaining, secondsRun));

        Indexer idxer = new Indexer(this, url, indexingServers, indexingThreads, cycleSeconds, random());

        Queries queries = new Queries(this, url, queryServers, queryThreads, random());

        idxer.waitOnThreads();

        queries.waitOnThreads();

        secondsRemaining = Math.max(secondsRemaining - resetInterval, 0);

        checkResults(queryServers.get(0), queries, idxer);

        secondsRun += cycleSeconds;

        if (secondsRemaining > 0) {
          deleteAllDocuments(queryServers.get(0), queries);
        }
      } while (secondsRemaining > 0);

      assertTrue("We didn't index any documents, somethings really messed up", cumulativeDocs > 0);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Caught unexpected exception");
    }
  }

  private void makeCores(File home, boolean oldStyle) throws Exception {
    File testSrcRoot = new File(SolrTestCaseJ4.TEST_HOME());
    String srcSolrXml = "solr-stress-new.xml";

    if (oldStyle) {
      srcSolrXml = "solr-stress-old.xml";
    }
    FileUtils.copyFile(new File(testSrcRoot, srcSolrXml), new File(home, "solr.xml"));

    // create directories in groups of 100 until you have enough.
    for (int idx = 0; idx < numCores; ++idx) {
      String coreName = String.format(Locale.ROOT, "%05d_core", idx);
      makeCore(new File(home, coreName), testSrcRoot, coreName);
      coreCounts.put(coreName, 0L);
      coreNames.add(coreName);
    }
  }

  private void makeCore(File coreDir, File testSrcRoot, String coreName) throws IOException {
    File conf = new File(coreDir, "conf");

    if (!conf.mkdirs()) log.warn("mkdirs returned false in makeCore... ignoring");

    File testConf = new File(testSrcRoot, "collection1/conf");

    FileUtils.copyFile(new File(testConf, "schema-tiny.xml"), new File(conf, "schema-tiny.xml"));

    FileUtils.copyFile(new File(testConf, "solrconfig-minimal.xml"), new File(conf, "solrconfig-minimal.xml"));
    FileUtils.copyFile(new File(testConf, "solrconfig.snippet.randomindexconfig.xml"),
        new File(conf, "solrconfig.snippet.randomindexconfig.xml"));

    FileUtils.copyFile(new File(testSrcRoot, "conf/core.properties"), new File(coreDir, "core.properties"));
  }


  void deleteAllDocuments(HttpSolrServer server, Queries queries) {
    log.info("Deleting data from last cycle, this may take a few minutes.");

    for (String core : coreNames) {
      try {
        server.setBaseURL(url + core);
        server.deleteByQuery("*:*");
        server.optimize(true, true); // should be close to a no-op.
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    // We're testing, after all. Let's be really sure things are as we expect.
    log.info("Insuring all cores empty");
    long foundDocs = 0;
    for (String core : coreNames) {
      try {
        long found = queries.getCount(server, core);
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

  private void checkResults(HttpSolrServer server, Queries queries, Indexer idxer) throws InterruptedException {
    log.info("Checking if indexes have all the documents they should...");
    long totalDocsFound = 0;
    for (Map.Entry<String, Long> ent : coreCounts.entrySet()) {
      server.setBaseURL(url + ent.getKey());
      for (int idx = 0; idx < 3; ++idx) {
        try {
          server.commit(true, true);
          break; // retry loop
        } catch (Exception e) {
          log.warn("Exception when committing core " + ent.getKey() + " " + e.getMessage());
          Thread.sleep(100L);
        }
      }
      long numFound = queries.getCount(server, ent.getKey());
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
  static volatile long stopTime;

  static AtomicInteger idUnique = new AtomicInteger(0);

  static AtomicInteger errors = new AtomicInteger(0);

  static AtomicInteger docsThisCycle = new AtomicInteger(0);

  static AtomicLong qTimesAccum = new AtomicLong(0);

  static AtomicInteger updateCounts = new AtomicInteger(0);

  static volatile int lastCount;
  static volatile long nextTime;

  ArrayList<OneIndexer> _threads = new ArrayList<OneIndexer>();

  public Indexer(OpenCloseCoreStressTest OCCST, String url, List<HttpSolrServer> servers, int numThreads, int secondsToRun, Random random) {
    stopTime = System.currentTimeMillis() + (secondsToRun * 1000);
    nextTime = System.currentTimeMillis() + 60000;
    docsThisCycle.set(0);
    qTimesAccum.set(0);
    updateCounts.set(0);
    for (int idx = 0; idx < numThreads; ++idx) {
      OneIndexer one = new OneIndexer(OCCST, url, servers.get(idx), random.nextLong());
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
    if (nextTime - System.currentTimeMillis() <= 0) {
      SolrTestCaseJ4.log.info(String.format(Locale.ROOT, " s indexed: [run %,8d] [cycle %,8d] [last minute %,8d] Last core updated: %s. Seconds left in cycle %,4d",
          myId, docsThisCycle.get(), myId - lastCount, core, stopTime - (System.currentTimeMillis() / 1000)));
      lastCount = myId;
      nextTime += (System.currentTimeMillis() / 1000) * 60;
    }
  }

}

class OneIndexer extends Thread {
  private final OpenCloseCoreStressTest OCCST;
  private final HttpSolrServer server;
  private final String baseUrl;
  private final Random random;

  OneIndexer(OpenCloseCoreStressTest OCCST, String url, HttpSolrServer server, long seed) {
    this.OCCST = OCCST;
    this.server = server;
    this.baseUrl = url;
    this.random = new Random(seed);
  }

  @Override
  public void run() {
    SolrTestCaseJ4.log.info(String.format(Locale.ROOT, "Starting indexing thread: " + getId()));

    while (Indexer.stopTime > System.currentTimeMillis()) {
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
          server.setBaseURL(baseUrl + core);
          UpdateResponse response = server.add(doc, OpenCloseCoreStressTest.COMMIT_WITHIN);
          if (response.getStatus() != 0) {
            SolrTestCaseJ4.log.warn("Failed to index a document to core " + core + " with status " + response.getStatus());
          } else {
            Indexer.qTimesAccum.addAndGet(response.getQTime());
            Indexer.updateCounts.incrementAndGet();
            break; // retry loop.
          }
          server.commit(true, true);
          Thread.sleep(100L); // Let's not go crazy here.
        } catch (Exception e) {
          if (e instanceof InterruptedException) return;
          Indexer.errors.incrementAndGet();
          if (idx == 2) {
            SolrTestCaseJ4.log.warn("Could not reach server while indexing for three tries, quitting " + e.getMessage());
          } else {
            SolrTestCaseJ4.log.info("Indexing thread " + Thread.currentThread().getId() + " swallowed one exception " + e.getMessage());
            try {
              Thread.sleep(100);
            } catch (InterruptedException tex) {
              return;
            }
          }
        }
      }
    }
    SolrTestCaseJ4.log.info("Leaving indexing thread " + getId());
  }
}

class Queries {
  static AtomicBoolean _keepon = new AtomicBoolean(true);

  List<Thread> _threads = new ArrayList<Thread>();
  static AtomicInteger _errors = new AtomicInteger(0);
  String baseUrl;

  public Queries(OpenCloseCoreStressTest OCCST, String url, List<HttpSolrServer> servers, int numThreads, Random random) {
    baseUrl = url;
    for (int idx = 0; idx < numThreads; ++idx) {
      Thread one = new OneQuery(OCCST, url, servers.get(idx), random.nextLong());
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

  public long getCount(HttpSolrServer server, String core) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", "/select");
    params.set("q", "*:*");
    long numFound = 0;
    server.setBaseURL(baseUrl + core);
    try {
      QueryResponse response = server.query(params);
      numFound = response.getResults().getNumFound();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return numFound;
  }
}

class OneQuery extends Thread {
  OpenCloseCoreStressTest OCCST;
  private final HttpSolrServer server;
  private final String baseUrl;
  private final Random random;

  OneQuery(OpenCloseCoreStressTest OCCST, String url, HttpSolrServer server, long seed) {
    this.OCCST = OCCST;
    this.server = server;
    this.baseUrl = url;
    this.random = new Random(seed);
  }

  @Override
  public void run() {
    SolrTestCaseJ4.log.info(String.format(Locale.ROOT, "Starting query thread: " + getId()));
    while (Queries._keepon.get()) {
      String core = OCCST.getRandomCore(random);
      for (int idx = 0; idx < 3; ++idx) {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("qt", "/select");
        params.set("q", "*:*");

        try {
          // sleep between 250ms and 10000 ms
          Thread.sleep(100L); // Let's not go crazy here.
          server.setBaseURL(baseUrl + core);
          QueryResponse response = server.query(params);

          if (response.getStatus() != 0) {
            SolrTestCaseJ4.log.warn("Failed to query core " + core + " with status " + response.getStatus());
          }
            // Perhaps collect some stats here in future.
          break; // retry loop
        } catch (Exception e) {
          if (e instanceof InterruptedException) return;
          Queries._errors.incrementAndGet();
          if (idx == 2) {
            SolrTestCaseJ4.log.warn("Could not reach server while indexing for three tries, quitting " + e.getMessage());
          } else {
            SolrTestCaseJ4.log.info("Querying thread: " + Thread.currentThread().getId() + " swallowed exception: " + e.getMessage());
            try {
              Thread.sleep(250L);
            } catch (InterruptedException tex) {
              return;
            }
          }
        }
      }
    }
    SolrTestCaseJ4.log.info(String.format(Locale.ROOT, "Leaving query thread: " + getId()));
  }

}
