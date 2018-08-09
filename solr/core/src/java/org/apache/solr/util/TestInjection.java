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
package org.apache.solr.util;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.NonExistentCoreException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.update.SolrIndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.handler.ReplicationHandler.CMD_DETAILS;
import static org.apache.solr.handler.ReplicationHandler.COMMAND;


/**
 * Allows random faults to be injected in running code during test runs.
 * 
 * Set static strings to "true" or "false" or "true:60" for true 60% of the time.
 * 
 * All methods are No-Ops unless <code>LuceneTestCase</code> is loadable via the ClassLoader used 
 * to load this class.  <code>LuceneTestCase.random()</code> is used as the source of all entropy.
 * 
 * @lucene.internal
 */
public class TestInjection {
  
  public static class TestShutdownFailError extends OutOfMemoryError {

    public TestShutdownFailError(String msg) {
      super(msg);
    }
    
  }
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static final Pattern ENABLED_PERCENT = Pattern.compile("(true|false)(?:\\:(\\d+))?$", Pattern.CASE_INSENSITIVE);
  
  private static final String LUCENE_TEST_CASE_FQN = "org.apache.lucene.util.LuceneTestCase";

  /** 
   * If null, then we are not being run as part of a test, and all TestInjection events should be No-Ops.
   * If non-null, then this class should be used for accessing random entropy
   * @see #random
   */
  private static final Class LUCENE_TEST_CASE;
  
  static {
    Class nonFinalTemp = null;
    try {
      ClassLoader classLoader = MethodHandles.lookup().lookupClass().getClassLoader();
      nonFinalTemp = classLoader.loadClass(LUCENE_TEST_CASE_FQN);
    } catch (ClassNotFoundException e) {
      log.debug("TestInjection methods will all be No-Ops since LuceneTestCase not found");
    }
    LUCENE_TEST_CASE = nonFinalTemp;
  }

  /**
   * Returns a random to be used by the current thread if available, otherwise
   * returns null.
   * @see #LUCENE_TEST_CASE
   */
  static Random random() { // non-private for testing
    if (null == LUCENE_TEST_CASE) {
      return null;
    } else {
      try {
        Method randomMethod = LUCENE_TEST_CASE.getMethod("random");
        return (Random) randomMethod.invoke(null);
      } catch (Exception e) {
        throw new IllegalStateException("Unable to use reflection to invoke LuceneTestCase.random()", e);
      }
    }
  }
  
  public static String nonGracefullClose = null;

  public static String failReplicaRequests = null;
  
  public static String failUpdateRequests = null;

  public static String nonExistentCoreExceptionAfterUnload = null;

  public static String updateLogReplayRandomPause = null;
  
  public static String updateRandomPause = null;

  public static String prepRecoveryOpPauseForever = null;

  public static String randomDelayInCoreCreation = null;
  
  public static int randomDelayMaxInCoreCreationInSec = 10;

  public static String splitFailureBeforeReplicaCreation = null;

  public static String splitFailureAfterReplicaCreation = null;

  public static String waitForReplicasInSync = "true:60";

  public static String failIndexFingerprintRequests = null;

  public static String wrongIndexFingerprint = null;
  
  private static Set<Timer> timers = Collections.synchronizedSet(new HashSet<Timer>());

  private static AtomicInteger countPrepRecoveryOpPauseForever = new AtomicInteger(0);

  public static Integer delayBeforeSlaveCommitRefresh=null;

  public static boolean uifOutOfMemoryError = false;

  public static void reset() {
    nonGracefullClose = null;
    failReplicaRequests = null;
    failUpdateRequests = null;
    nonExistentCoreExceptionAfterUnload = null;
    updateLogReplayRandomPause = null;
    updateRandomPause = null;
    randomDelayInCoreCreation = null;
    splitFailureBeforeReplicaCreation = null;
    splitFailureAfterReplicaCreation = null;
    prepRecoveryOpPauseForever = null;
    countPrepRecoveryOpPauseForever = new AtomicInteger(0);
    waitForReplicasInSync = "true:60";
    failIndexFingerprintRequests = null;
    wrongIndexFingerprint = null;
    delayBeforeSlaveCommitRefresh = null;
    uifOutOfMemoryError = false;

    for (Timer timer : timers) {
      timer.cancel();
    }
  }

  public static boolean injectWrongIndexFingerprint() {
    if (wrongIndexFingerprint != null)  {
      Random rand = random();
      if (null == rand) return true;

      Pair<Boolean,Integer> pair = parseValue(wrongIndexFingerprint);
      boolean enabled = pair.first();
      int chanceIn100 = pair.second();
      if (enabled && rand.nextInt(100) >= (100 - chanceIn100)) {
        return true;
      }
    }
    return false;
  }

  public static boolean injectFailIndexFingerprintRequests()  {
    if (failIndexFingerprintRequests != null) {
      Random rand = random();
      if (null == rand) return true;

      Pair<Boolean,Integer> pair = parseValue(failIndexFingerprintRequests);
      boolean enabled = pair.first();
      int chanceIn100 = pair.second();
      if (enabled && rand.nextInt(100) >= (100 - chanceIn100)) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Random test index fingerprint fail");
      }
    }
    return true;
  }
  
  public static boolean injectRandomDelayInCoreCreation() {
    if (randomDelayInCoreCreation != null) {
      Random rand = random();
      if (null == rand) return true;
      
      Pair<Boolean,Integer> pair = parseValue(randomDelayInCoreCreation);
      boolean enabled = pair.first();
      int chanceIn100 = pair.second();
      if (enabled && rand.nextInt(100) >= (100 - chanceIn100)) {
        int delay = rand.nextInt(randomDelayMaxInCoreCreationInSec);
        log.info("Inject random core creation delay of {}s", delay);
        try {
          Thread.sleep(delay * 1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
    return true;
  }
  
  public static boolean injectNonGracefullClose(CoreContainer cc) {
    if (cc.isShutDown() && nonGracefullClose != null) {
      Random rand = random();
      if (null == rand) return true;
      
      Pair<Boolean,Integer> pair = parseValue(nonGracefullClose);
      boolean enabled = pair.first();
      int chanceIn100 = pair.second();
      if (enabled && rand.nextInt(100) >= (100 - chanceIn100)) {
        if (rand.nextBoolean()) {
          throw new TestShutdownFailError("Test exception for non graceful close");
        } else {
          
          final Thread cthread = Thread.currentThread();
          TimerTask task = new TimerTask() {
            @Override
            public void run() {
              // as long as places that catch interruptedexception reset that
              // interrupted status,
              // we should only need to do it once
              
              try {
                // call random() again to get the correct one for this thread
                Random taskRand = random();
                Thread.sleep(taskRand.nextInt(1000));
              } catch (InterruptedException e) {
              
              }
              
              cthread.interrupt();
              timers.remove(this);
              cancel();
            }
          };
          Timer timer = new Timer();
          timers.add(timer);
          timer.schedule(task, rand.nextInt(500));
        }
      }
    }
    return true;
  }

  public static boolean injectFailReplicaRequests() {
    if (failReplicaRequests != null) {
      Random rand = random();
      if (null == rand) return true;
      
      Pair<Boolean,Integer> pair = parseValue(failReplicaRequests);
      boolean enabled = pair.first();
      int chanceIn100 = pair.second();
      if (enabled && rand.nextInt(100) >= (100 - chanceIn100)) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Random test update fail");
      }
    }

    return true;
  }
  
  public static boolean injectFailUpdateRequests() {
    if (failUpdateRequests != null) {
      Random rand = random();
      if (null == rand) return true;
      
      Pair<Boolean,Integer> pair = parseValue(failUpdateRequests);
      boolean enabled = pair.first();
      int chanceIn100 = pair.second();
      if (enabled && rand.nextInt(100) >= (100 - chanceIn100)) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Random test update fail");
      }
    }

    return true;
  }
  
  public static boolean injectNonExistentCoreExceptionAfterUnload(String cname) {
    if (nonExistentCoreExceptionAfterUnload != null) {
      Random rand = random();
      if (null == rand) return true;
      
      Pair<Boolean,Integer> pair = parseValue(nonExistentCoreExceptionAfterUnload);
      boolean enabled = pair.first();
      int chanceIn100 = pair.second();
      if (enabled && rand.nextInt(100) >= (100 - chanceIn100)) {
        throw new NonExistentCoreException("Core not found to unload: " + cname);
      }
    }

    return true;
  }
  
  public static boolean injectUpdateLogReplayRandomPause() {
    if (updateLogReplayRandomPause != null) {
      Random rand = random();
      if (null == rand) return true;
      
      Pair<Boolean,Integer> pair = parseValue(updateLogReplayRandomPause);
      boolean enabled = pair.first();
      int chanceIn100 = pair.second();
      if (enabled && rand.nextInt(100) >= (100 - chanceIn100)) {
        long rndTime = rand.nextInt(1000);
        log.info("inject random log replay delay of {}ms", rndTime);
        try {
          Thread.sleep(rndTime);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    return true;
  }
  
  public static boolean injectUpdateRandomPause() {
    if (updateRandomPause != null) {
      Random rand = random();
      if (null == rand) return true;
      
      Pair<Boolean,Integer> pair = parseValue(updateRandomPause);
      boolean enabled = pair.first();
      int chanceIn100 = pair.second();
      if (enabled && rand.nextInt(100) >= (100 - chanceIn100)) {
        long rndTime;
        if (rand.nextInt(10) > 2) {
          rndTime = rand.nextInt(300);
        } else {
          rndTime = rand.nextInt(1000);
        }
       
        log.info("inject random update delay of {}ms", rndTime);
        try {
          Thread.sleep(rndTime);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    return true;
  }

  public static boolean injectPrepRecoveryOpPauseForever() {
    if (prepRecoveryOpPauseForever != null)  {
      Random rand = random();
      if (null == rand) return true;

      Pair<Boolean,Integer> pair = parseValue(prepRecoveryOpPauseForever);
      boolean enabled = pair.first();
      int chanceIn100 = pair.second();
      // Prevent for continuous pause forever
      if (enabled && rand.nextInt(100) >= (100 - chanceIn100) && countPrepRecoveryOpPauseForever.get() < 1) {
        countPrepRecoveryOpPauseForever.incrementAndGet();
        log.info("inject pause forever for prep recovery op");
        try {
          Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      } else {
        countPrepRecoveryOpPauseForever.set(0);
      }
    }

    return true;
  }

  private static boolean injectSplitFailure(String probability, String label) {
    if (probability != null)  {
      Random rand = random();
      if (null == rand) return true;

      Pair<Boolean,Integer> pair = parseValue(probability);
      boolean enabled = pair.first();
      int chanceIn100 = pair.second();
      if (enabled && rand.nextInt(100) >= (100 - chanceIn100)) {
        log.info("Injecting failure: " + label);
        throw new SolrException(ErrorCode.SERVER_ERROR, "Error: " + label);
      }
    }
    return true;
  }

  public static boolean injectSplitFailureBeforeReplicaCreation() {
    return injectSplitFailure(splitFailureBeforeReplicaCreation, "before creating replica for sub-shard");
  }

  public static boolean injectSplitFailureAfterReplicaCreation() {
    return injectSplitFailure(splitFailureAfterReplicaCreation, "after creating replica for sub-shard");
  }

  @SuppressForbidden(reason = "Need currentTimeMillis, because COMMIT_TIME_MSEC_KEY use currentTimeMillis as value")
  public static boolean waitForInSyncWithLeader(SolrCore core, ZkController zkController, String collection, String shardId) {
    if (waitForReplicasInSync == null) return true;
    log.info("Start waiting for replica in sync with leader");
    long currentTime = System.currentTimeMillis();
    Pair<Boolean,Integer> pair = parseValue(waitForReplicasInSync);
    boolean enabled = pair.first();
    if (!enabled) return true;
    long t = System.currentTimeMillis() - 200;
    for (int i = 0; i < pair.second(); i++) {
      try {
        if (core.isClosed()) return true;
        Replica leaderReplica = zkController.getZkStateReader().getLeaderRetry(
            collection, shardId);
        try (HttpSolrClient leaderClient = new HttpSolrClient.Builder(leaderReplica.getCoreUrl()).build()) {
          ModifiableSolrParams params = new ModifiableSolrParams();
          params.set(CommonParams.QT, ReplicationHandler.PATH);
          params.set(COMMAND, CMD_DETAILS);
  
          NamedList<Object> response = leaderClient.request(new QueryRequest(params));
          long leaderVersion = (long) ((NamedList)response.get("details")).get("indexVersion");
          String localVersion = core.withSearcher(searcher ->
              searcher.getIndexReader().getIndexCommit().getUserData().get(SolrIndexWriter.COMMIT_TIME_MSEC_KEY));
          if (localVersion == null && leaderVersion == 0 && !core.getUpdateHandler().getUpdateLog().hasUncommittedChanges())
            return true;
          if (localVersion != null && Long.parseLong(localVersion) == leaderVersion && (leaderVersion >= t || i >= 6)) {
            log.info("Waiting time for tlog replica to be in sync with leader: {}", System.currentTimeMillis()-currentTime);
            return true;
          } else {
            log.debug("Tlog replica not in sync with leader yet. Attempt: {}. Local Version={}, leader Version={}", i, localVersion, leaderVersion);
            Thread.sleep(500);
          }
  
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        if (core.isClosed()) return true;
        log.error("Thread interrupted while waiting for core {} to be in sync with leader", core.getName());
        return false;
      } catch (Exception e) {
        if (core.isClosed()) return true;
        log.error("Exception when wait for replicas in sync with master. Will retry until timeout.", e);
      }
    }
    return false;
  }
  
  private static Pair<Boolean,Integer> parseValue(String raw) {
    Matcher m = ENABLED_PERCENT.matcher(raw);
    if (!m.matches()) throw new RuntimeException("No match, probably bad syntax: " + raw);
    String val = m.group(1);
    String percent = "100";
    if (m.groupCount() == 2) {
      percent = m.group(2);
    }
    return new Pair<>(Boolean.parseBoolean(val), Integer.parseInt(percent));
  }

  public static boolean injectDelayBeforeSlaveCommitRefresh() {
    if (delayBeforeSlaveCommitRefresh!=null) {
      try {
        log.info("Pausing IndexFetcher for {}ms", delayBeforeSlaveCommitRefresh);
        Thread.sleep(delayBeforeSlaveCommitRefresh);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    return true;
  }

  public static boolean injectUIFOutOfMemoryError() {
    if (uifOutOfMemoryError ) {
      throw new OutOfMemoryError("Test Injection");
    }
    return true;
  }

}
