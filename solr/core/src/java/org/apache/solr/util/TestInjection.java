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

import org.apache.solr.common.NonExistentCoreException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.Pair;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  
  private static Set<Timer> timers = Collections.synchronizedSet(new HashSet<Timer>());

  private static AtomicInteger countPrepRecoveryOpPauseForever = new AtomicInteger(0);

  public static void reset() {
    nonGracefullClose = null;
    failReplicaRequests = null;
    failUpdateRequests = null;
    nonExistentCoreExceptionAfterUnload = null;
    updateLogReplayRandomPause = null;
    updateRandomPause = null;
    randomDelayInCoreCreation = null;
    splitFailureBeforeReplicaCreation = null;
    prepRecoveryOpPauseForever = null;
    countPrepRecoveryOpPauseForever = new AtomicInteger(0);

    for (Timer timer : timers) {
      timer.cancel();
    }
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
        long rndTime = rand.nextInt(1000);
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
      if (enabled && rand.nextInt(100) >= (100 - chanceIn100) && countPrepRecoveryOpPauseForever.get() < 2) {
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

  public static boolean injectSplitFailureBeforeReplicaCreation() {
    if (splitFailureBeforeReplicaCreation != null)  {
      Random rand = random();
      if (null == rand) return true;

      Pair<Boolean,Integer> pair = parseValue(splitFailureBeforeReplicaCreation);
      boolean enabled = pair.first();
      int chanceIn100 = pair.second();
      if (enabled && rand.nextInt(100) >= (100 - chanceIn100)) {
        log.info("Injecting failure in creating replica for sub-shard");
        throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to create replica");
      }
    }

    return true;
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

}
