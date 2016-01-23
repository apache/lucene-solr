package org.apache.solr.util;

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

import java.util.Collections;
import java.util.HashSet;

import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.Pair;
import org.apache.solr.core.CoreContainer;


/**
 * Allows random faults to be injected in running code during test runs.
 */
public class TestInjection {
  
  public static class TestShutdownFailError extends OutOfMemoryError {

    public TestShutdownFailError(String msg) {
      super(msg);
    }
    
  }
  
  private static final Pattern ENABLED_PERCENT = Pattern.compile("(true|false)(?:\\:(\\d+))?$", Pattern.CASE_INSENSITIVE);
  private static final Random RANDOM;
  
  static {
    // We try to make things reproducible in the context of our tests by initializing the random instance
    // based on the current seed
    String seed = System.getProperty("tests.seed");
    if (seed == null) {
      RANDOM = new Random();
    } else {
      RANDOM = new Random(seed.hashCode());
    }
  }
  
  public static String nonGracefullClose = null;

  public static String failReplicaRequests = null;
  
  public static String failUpdateRequests = null;
  
  private static Set<Timer> timers = Collections.synchronizedSet(new HashSet<Timer>());


  public static void reset() {
    nonGracefullClose = null;
    failReplicaRequests = null;
    failUpdateRequests = null;

    for (Timer timer : timers) {
      timer.cancel();
    }
  }
  
  public static boolean injectNonGracefullClose(CoreContainer cc) {
    if (cc.isShutDown() && nonGracefullClose != null) {
      Pair<Boolean,Integer> pair = parseValue(nonGracefullClose);
      boolean enabled = pair.getKey();
      int chanceIn100 = pair.getValue();
      if (enabled && RANDOM.nextInt(100) >= (100 - chanceIn100)) {
        if (RANDOM.nextBoolean()) {
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
                Thread.sleep(RANDOM.nextInt(1000));
              } catch (InterruptedException e) {
              
              }
              
              cthread.interrupt();
              timers.remove(this);
              cancel();
            }
          };
          Timer timer = new Timer();
          timers.add(timer);
          timer.schedule(task, RANDOM.nextInt(500));
        }
      }
    }
    return true;
  }

  public static boolean injectFailReplicaRequests() {
    if (failReplicaRequests != null) {
      Pair<Boolean,Integer> pair = parseValue(failReplicaRequests);
      boolean enabled = pair.getKey();
      int chanceIn100 = pair.getValue();
      if (enabled && RANDOM.nextInt(100) >= (100 - chanceIn100)) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Random test update fail");
      }
    }

    return true;
  }
  
  public static boolean injectFailUpdateRequests() {
    if (failUpdateRequests != null) {
      Pair<Boolean,Integer> pair = parseValue(failUpdateRequests);
      boolean enabled = pair.getKey();
      int chanceIn100 = pair.getValue();
      if (enabled && RANDOM.nextInt(100) >= (100 - chanceIn100)) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Random test update fail");
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
